/**
 * Mulberry32 PRNG - fast, simple seeded random number generator
 * Produces deterministic sequences based on seed
 */
export class SeededRandom {
  private state: number;

  constructor(seed: number) {
    this.state = seed;
  }

  /**
   * Returns a random 32-bit unsigned integer
   */
  nextUint32(): number {
    let t = (this.state += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0);
  }

  /**
   * Fill a buffer with seeded random bytes (uncompressible)
   */
  fillBuffer(buffer: Buffer): void {
    // Fill 4 bytes at a time for efficiency
    const uint32Count = Math.floor(buffer.length / 4);
    for (let i = 0; i < uint32Count; i++) {
      buffer.writeUInt32LE(this.nextUint32(), i * 4);
    }
    // Fill remaining bytes
    const remaining = buffer.length % 4;
    if (remaining > 0) {
      const lastValue = this.nextUint32();
      for (let i = 0; i < remaining; i++) {
        buffer[uint32Count * 4 + i] = (lastValue >> (i * 8)) & 0xff;
      }
    }
  }
}
