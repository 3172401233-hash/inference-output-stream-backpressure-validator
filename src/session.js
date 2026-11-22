import { ClientConsumer } from './consumer.js';

export const TokenState = {
  QUEUED: 'queued',       // 在输出缓存中，可投递
  RELEASED: 'released',   // 所有在线消费者都已越过，缓存已释放
  DROPPED: 'dropped',     // 终止时未发送，状态明确
};

export class InferenceSession {
  constructor({
    id = 'req-1',
    generationRate = 2,   // 每 tick 生成的 token 数（生成速度）
    totalTokens = 30,     // 本次请求计划生成的 token 总数
    highWatermark = 8,    // 队列达到该深度暂停生成
    lowWatermark = 4,     // 队列回落到该深度恢复生成
    maxQueueSize = 16,    // 队列硬上限，达到即终止请求
    deadline = 200,       // tick 截止时间
    maxConsumerLag = 12,  // 消费者落后超过该值则被驱逐，避免拖垮其他消费者
  } = {}) {
    this.id = id;
    this.generationRate = generationRate;
    this.totalTokens = totalTokens;
    this.highWatermark = highWatermark;
    this.lowWatermark = lowWatermark;
    this.maxQueueSize = maxQueueSize;
    this.deadline = deadline;
    this.maxConsumerLag = maxConsumerLag;

    this.now = 0;
    this.tokens = [];        // 输出缓存（多个消费者共享的结果流）
    this.baseSeq = 0;        // tokens[0] 对应的 seq
    this.nextSeq = 0;
    this.generated = 0;
    this.paused = false;
    this.state = 'active';   // active | terminated
    this.terminationReason = null;
    this.cancelRequested = false;
    this.cancelReason = null;

    this.consumers = new Map();
    this.kvCacheRefs = 1;    // 生成期间持有 KV 缓存引用
    this.kvCacheReleased = false;

    this.log = [];           // 每 tick 的决策记录
    this.evidence = [];      // 不一致证据
    this.pausePoints = [];   // 暂停点
    this.peakQueueDepth = 0;
    this.cacheReleasedTotal = 0;
  }

  get headSeq() {
    return this.nextSeq;
  }

  get queueDepth() {
    return this.tokens.length;
  }

  attachConsumer(consumer, { resumeFrom = null } = {}) {
    const requested = resumeFrom ?? this.headSeq;
    if (requested < this.baseSeq) {
      // 请求续传的位置已被缓存释放：已发送/已淘汰的 token 不能重发。
      this.evidence.push({
        type: 'consumer-gap',
        consumerId: consumer.id,
        requested,
        baseSeq: this.baseSeq,
        tick: this.now,
      });
    }
    consumer.cursor = Math.max(requested, this.baseSeq);
    consumer.connected = true;
    this.consumers.set(consumer.id, consumer);
    return consumer;
  }

  detachConsumer(id) {
    const consumer = this.consumers.get(id);
    if (consumer) consumer.connected = false;
    this.consumers.delete(id);
    return consumer ?? null;
  }

  cancel(reason = 'cancelled') {
    this.cancelRequested = true;
    this.cancelReason = reason;
  }

  tick() {
    if (this.state === 'terminated') return null;
    this.now += 1;
    this._generate();
    for (const consumer of this.consumers.values()) this._flush(consumer);
    for (const consumer of this.consumers.values()) consumer.drain();
    this._evictLaggingConsumers();
    this._releaseCache();
    this.peakQueueDepth = Math.max(this.peakQueueDepth, this.queueDepth);
    return this._decide();
  }

  run(maxTicks = this.deadline + 1) {
    let decision = null;
    for (let i = 0; i < maxTicks && this.state !== 'terminated'; i += 1) {
      decision = this.tick();
    }
    return decision;
  }

  _generate() {
    if (this.paused) return;
    for (let i = 0; i < this.generationRate && this.generated < this.totalTokens; i += 1) {
      this.tokens.push({
        seq: this.nextSeq,
        text: `tok-${this.nextSeq}`,
        state: TokenState.QUEUED,
      });
      this.nextSeq += 1;
      this.generated += 1;
    }
  }

  _flush(consumer) {
    if (!consumer.connected) return;
    if (consumer.cursor < this.baseSeq) {
      // 消费者游标落后于缓存起点：中间 token 已释放，无法补发。
      this.evidence.push({
        type: 'consumer-gap',
        consumerId: consumer.id,
        requested: consumer.cursor,
        baseSeq: this.baseSeq,
        tick: this.now,
      });
      consumer.cursor = this.baseSeq;
    }
    while (consumer.cursor < this.headSeq) {
      const token = this.tokens[consumer.cursor - this.baseSeq];
      if (!token || token.state !== TokenState.QUEUED) {
        consumer.cursor += 1;
        continue;
      }
      if (consumer.sent.includes(token.seq)) {
        // 已发送 token 不能重发。
        this.evidence.push({
          type: 'resend-attempt',
          consumerId: consumer.id,
          seq: token.seq,
          tick: this.now,
        });
        consumer.cursor += 1;
        continue;
      }
      if (!consumer.write(token)) break; // 背压：窗口耗尽或写缓冲已满
      consumer.cursor += 1;
    }
  }

  _evictLaggingConsumers() {
    for (const consumer of [...this.consumers.values()]) {
      const lag = this.headSeq - consumer.cursor;
      if (lag > this.maxConsumerLag) {
        for (let seq = consumer.cursor; seq < this.headSeq; seq += 1) {
          consumer.dropped.push(seq);
        }
        this.evidence.push({
          type: 'consumer-evicted',
          consumerId: consumer.id,
          lag,
          tick: this.now,
        });
        this.detachConsumer(consumer.id);
      }
    }
  }

  _releaseCache() {
    if (this.consumers.size === 0) return; // 无在线消费者时保留缓存，等待重连
    let minCursor = Infinity;
    for (const consumer of this.consumers.values()) {
      minCursor = Math.min(minCursor, consumer.cursor);
    }
    while (this.tokens.length > 0 && this.baseSeq < minCursor) {
      const token = this.tokens.shift();
      token.state = TokenState.RELEASED;
      this.baseSeq += 1;
      this.cacheReleasedTotal += 1;
    }
  }

  _decide() {
    if (this.cancelRequested) return this._terminate(this.cancelReason ?? 'cancelled');
    if (this.now >= this.deadline) return this._terminate('deadline');
    if (this.queueDepth >= this.maxQueueSize) return this._terminate('queue-overflow');
    if (this.generated >= this.totalTokens && this.queueDepth === 0) {
      const buffersEmpty = [...this.consumers.values()]
        .every((c) => c.writeBuffer.length === 0);
      if (buffersEmpty) return this._terminate('completed');
    }
    if (!this.paused && this.queueDepth >= this.highWatermark) {
      this.paused = true;
      this.pausePoints.push({ tick: this.now, queueDepth: this.queueDepth });
      return this._record('pause-generation');
    }
    if (this.paused) {
      if (this.queueDepth <= this.lowWatermark) {
        this.paused = false;
        return this._record('resume-generation');
      }
      return this._record('paused');
    }
    const backpressured = [...this.consumers.values()]
      .some((c) => c.connected && (c.window <= 0 || c.writeBuffer.length >= c.bufferMax));
    if (backpressured) return this._record('continue-buffering');
    return this._record('stream');
  }

  _terminate(reason) {
    this.state = 'terminated';
    this.terminationReason = reason;
    // 未发送 token 给出明确状态。
    for (const token of this.tokens) {
      if (token.state === TokenState.QUEUED) {
        token.state = TokenState.DROPPED;
        token.dropReason = reason;
        token.droppedAt = this.now;
      }
    }
    for (const consumer of this.consumers.values()) {
      for (const token of consumer.writeBuffer) {
        consumer.dropped.push(token.seq);
      }
      consumer.writeBuffer = [];
    }
    // 释放 KV 缓存引用。
    this.kvCacheRefs = 0;
    this.kvCacheReleased = true;
    return this._record('terminate', { reason });
  }

  _record(action, extra = {}) {
    const entry = {
      tick: this.now,
      action,
      queueDepth: this.queueDepth,
      baseSeq: this.baseSeq,
      kvCacheRefs: this.kvCacheRefs,
      consumers: [...this.consumers.values()].map((c) => ({
        id: c.id,
        window: c.window,
        buffered: c.writeBuffer.length,
        cursor: c.cursor,
      })),
      ...extra,
    };
    this.log.push(entry);
    return entry;
  }
}

export { ClientConsumer };
