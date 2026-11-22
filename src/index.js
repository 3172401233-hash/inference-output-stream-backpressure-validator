import { InferenceSession, ClientConsumer, TokenState } from './session.js';

export { InferenceSession, ClientConsumer, TokenState };

// 本地模拟演示：一个快消费者 + 一个慢消费者共享结果流。
// 所有“网络写入”均为内存模拟，见 README.md。
if (import.meta.url === `file://${process.argv[1]}`) {
  const session = new InferenceSession({
    id: 'demo',
    generationRate: 3,
    totalTokens: 24,
    highWatermark: 8,
    lowWatermark: 3,
    maxQueueSize: 24,
    deadline: 100,
    maxConsumerLag: 10,
  });
  const fast = new ClientConsumer({ id: 'fast', window: 4, drainRate: 3 });
  const slow = new ClientConsumer({ id: 'slow', window: 2, drainRate: 0 });
  session.attachConsumer(fast);
  session.attachConsumer(slow);
  for (let i = 0; i < 100 && session.state !== 'terminated'; i += 1) {
    session.tick();
    if (session.now === 30) slow.drainRate = 2; // 慢消费者恢复读取
  }

  console.log('termination:', session.terminationReason);
  console.log('peak queue depth:', session.peakQueueDepth);
  console.log('cache released:', session.cacheReleasedTotal);
  console.log('kv cache released:', session.kvCacheReleased);
  console.log('fast sent:', fast.sent.length, 'slow sent:', slow.sent.length,
    'slow dropped:', slow.dropped.length);
  console.log('pause points:', session.pausePoints);
  console.log('evidence:', session.evidence);
  console.log('decisions:', session.log.map((e) => `${e.tick}:${e.action}`).join(' '));
}
