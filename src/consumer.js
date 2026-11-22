// 客户端消费者（本地模拟）。
// 注意：所有“网络写入”都只是写入内存数组 writeBuffer / sent，
// 不创建任何 socket、不发起任何真实网络 IO。
export class ClientConsumer {
  constructor({ id, window = 4, bufferMax = 8, drainRate = 1 } = {}) {
    if (!id) throw new Error('consumer id is required');
    this.id = id;
    // 客户端窗口：还能接受多少 token 的写信用。
    this.window = window;
    this.maxWindow = window;
    // 写缓冲上限：模拟客户端内核/框架写缓冲。
    this.bufferMax = bufferMax;
    // 每 tick 客户端从写缓冲取走多少 token（0 表示客户端完全不读）。
    this.drainRate = drainRate;
    this.writeBuffer = []; // 已写入缓冲、尚未被客户端取走的 token
    this.sent = [];        // 已被客户端取走的 token seq（已发送，不可重发）
    this.dropped = [];     // 终止/驱逐时未能送达的 token seq
    this.cursor = null;    // 下一个待写入的 token seq
    this.connected = true;
  }

  // 模拟一次本地写入。返回 false 表示背压（窗口耗尽或缓冲已满）。
  write(token) {
    if (!this.connected) return false;
    if (this.window <= 0 || this.writeBuffer.length >= this.bufferMax) return false;
    this.writeBuffer.push(token);
    this.window -= 1;
    return true;
  }

  // 模拟客户端读取：把缓冲中的 token 标记为“已发送”，并归还窗口。
  drain() {
    const n = Math.min(this.drainRate, this.writeBuffer.length);
    for (let i = 0; i < n; i += 1) {
      const token = this.writeBuffer.shift();
      this.sent.push(token.seq);
      this.window += 1;
    }
    return n;
  }

  disconnect() {
    this.connected = false;
  }
}
