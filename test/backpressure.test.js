import test from 'node:test';
import assert from 'node:assert/strict';
import { InferenceSession, ClientConsumer, TokenState } from '../src/index.js';

function actions(session) {
  return session.log.map((e) => e.action);
}

test('慢消费者：背压导致暂停生成，恢复后继续，且不阻塞共享流中的快消费者', () => {
  const session = new InferenceSession({
    generationRate: 3,
    totalTokens: 40,
    highWatermark: 6,
    lowWatermark: 2,
    maxQueueSize: 40,
    deadline: 500,
    maxConsumerLag: 1000,
  });
  const fast = new ClientConsumer({ id: 'fast', window: 4, drainRate: 3 });
  const slow = new ClientConsumer({ id: 'slow', window: 2, drainRate: 0 });
  session.attachConsumer(fast);
  session.attachConsumer(slow);

  for (let i = 0; i < 40 && session.state !== 'terminated'; i += 1) session.tick();

  // 慢消费者窗口耗尽、写缓冲堆积，产生背压。
  assert.equal(slow.sent.length, 0);
  assert.ok(slow.writeBuffer.length > 0);
  // 队列达到高水位后出现暂停点。
  assert.ok(session.pausePoints.length > 0);
  assert.ok(actions(session).includes('pause-generation'));
  assert.ok(actions(session).includes('continue-buffering'));
  // 慢消费者不读，但快消费者已追平生成进度（共享流不被无限阻塞）。
  assert.ok(fast.sent.length > 0);
  assert.equal(fast.cursor, session.headSeq);
  assert.equal(session.paused, true);

  // 慢消费者恢复读取，队列回落后恢复生成，最终完成。
  slow.drainRate = 3;
  session.run(500);
  assert.equal(session.terminationReason, 'completed');
  assert.ok(actions(session).includes('resume-generation'));
  assert.equal(slow.sent.length, 40);
  assert.equal(fast.sent.length, 40);
  // 每个消费者收到的 seq 无重复（已发送 token 未重发）。
  assert.equal(new Set(fast.sent).size, fast.sent.length);
  assert.equal(new Set(slow.sent).size, slow.sent.length);
});

test('断线重连：已发送 token 不重发，未发送 token 续传', () => {
  const session = new InferenceSession({
    generationRate: 2,
    totalTokens: 20,
    highWatermark: 100,
    maxQueueSize: 100,
    deadline: 500,
    maxConsumerLag: 1000,
  });
  const client = new ClientConsumer({ id: 'c1', window: 3, drainRate: 1 });
  session.attachConsumer(client);

  for (let i = 0; i < 5; i += 1) session.tick();
  const sentBefore = [...client.sent];
  assert.ok(sentBefore.length > 0);
  const resumeCursor = client.cursor;

  // 断线期间服务端继续生成，队列保留未发送 token。
  session.detachConsumer('c1');
  for (let i = 0; i < 5; i += 1) session.tick();
  assert.ok(session.queueDepth > 0);

  // 重连后续传：从断线游标继续，已发送的不重发。
  session.attachConsumer(client, { resumeFrom: resumeCursor });
  session.run(500);

  assert.equal(session.terminationReason, 'completed');
  const sentAfter = client.sent.slice(sentBefore.length);
  for (const seq of sentBefore) {
    assert.ok(!sentAfter.includes(seq), `seq ${seq} 被重发`);
  }
  assert.equal(new Set(client.sent).size, client.sent.length);
  // 断线前未发送的 token（游标之后）最终都送达。
  for (let seq = resumeCursor; seq < 20; seq += 1) {
    assert.ok(client.sent.includes(seq), `seq ${seq} 丢失`);
  }
});

test('断线重连：缓存已释放的位置产生 consumer-gap 不一致证据', () => {
  const session = new InferenceSession({
    generationRate: 2,
    totalTokens: 30,
    highWatermark: 100,
    maxQueueSize: 100,
    deadline: 500,
    maxConsumerLag: 1000,
  });
  const stale = new ClientConsumer({ id: 'stale', window: 2, drainRate: 1 });
  const active = new ClientConsumer({ id: 'active', window: 4, drainRate: 4 });
  session.attachConsumer(stale);
  session.attachConsumer(active);

  session.tick();
  session.detachConsumer('stale');
  const staleCursor = stale.cursor;

  // 活跃消费者持续推进，缓存释放掉 stale 未消费的位置。
  for (let i = 0; i < 8; i += 1) session.tick();
  assert.ok(session.baseSeq > staleCursor);

  session.attachConsumer(stale, { resumeFrom: staleCursor });
  const gaps = session.evidence.filter((e) => e.type === 'consumer-gap' && e.consumerId === 'stale');
  assert.ok(gaps.length > 0);
  assert.equal(stale.cursor, session.baseSeq);
});

test('部分发送：取消时未发送 token 状态为 dropped，KV 缓存引用释放', () => {
  const session = new InferenceSession({
    generationRate: 4,
    totalTokens: 50,
    highWatermark: 100,
    maxQueueSize: 100,
    deadline: 500,
    maxConsumerLag: 1000,
  });
  const slow = new ClientConsumer({ id: 'slow', window: 2, drainRate: 0 });
  session.attachConsumer(slow);

  for (let i = 0; i < 4; i += 1) session.tick();
  session.cancel('client-aborted');
  session.tick();

  assert.equal(session.state, 'terminated');
  assert.equal(session.terminationReason, 'client-aborted');
  // 队列中未发送的 token 状态明确为 dropped。
  const queued = session.tokens.filter((t) => t.state === TokenState.QUEUED);
  assert.equal(queued.length, 0);
  const dropped = session.tokens.filter((t) => t.state === TokenState.DROPPED);
  assert.ok(dropped.length > 0);
  assert.ok(dropped.every((t) => t.dropReason === 'client-aborted'));
  // 写缓冲中未送达的 token 记入消费者 dropped。
  assert.ok(slow.dropped.length > 0);
  // KV 缓存引用已释放。
  assert.equal(session.kvCacheRefs, 0);
  assert.equal(session.kvCacheReleased, true);
});

test('队列上限：无消费者且持续生成，达到硬上限终止请求', () => {
  const session = new InferenceSession({
    generationRate: 4,
    totalTokens: 100,
    highWatermark: 1000,
    maxQueueSize: 10,
    deadline: 500,
  });
  session.run(500);

  assert.equal(session.state, 'terminated');
  assert.equal(session.terminationReason, 'queue-overflow');
  assert.ok(session.peakQueueDepth >= 10);
  const dropped = session.tokens.filter((t) => t.state === TokenState.DROPPED);
  assert.ok(dropped.length > 0);
  assert.equal(session.kvCacheReleased, true);
});

test('多消费者：落后过多的消费者被驱逐，其余消费者不受影响', () => {
  const session = new InferenceSession({
    generationRate: 2,
    totalTokens: 24,
    highWatermark: 1000,
    maxQueueSize: 1000,
    deadline: 500,
    maxConsumerLag: 6,
  });
  const fast = new ClientConsumer({ id: 'fast', window: 4, drainRate: 2 });
  const stuck = new ClientConsumer({ id: 'stuck', window: 1, drainRate: 0 });
  session.attachConsumer(fast);
  session.attachConsumer(stuck);
  session.run(500);

  assert.equal(session.terminationReason, 'completed');
  const evicted = session.evidence.filter(
    (e) => e.type === 'consumer-evicted' && e.consumerId === 'stuck',
  );
  assert.equal(evicted.length, 1);
  assert.equal(stuck.connected, false);
  // 被驱逐消费者未送达的 token 有明确记录。
  assert.ok(stuck.dropped.length > 0);
  // 快消费者收齐全部 token，缓存随其游标释放。
  assert.equal(fast.sent.length, 24);
  assert.ok(session.cacheReleasedTotal > 0);
});

test('deadline：超时终止，未发送 token 状态明确', () => {
  const session = new InferenceSession({
    generationRate: 1,
    totalTokens: 1000,
    highWatermark: 1000,
    maxQueueSize: 1000,
    deadline: 10,
  });
  session.run(500);

  assert.equal(session.terminationReason, 'deadline');
  assert.equal(session.now, 10);
  const dropped = session.tokens.filter((t) => t.state === TokenState.DROPPED);
  assert.ok(dropped.length > 0);
  assert.ok(dropped.every((t) => t.dropReason === 'deadline'));
  assert.equal(session.kvCacheRefs, 0);
});
