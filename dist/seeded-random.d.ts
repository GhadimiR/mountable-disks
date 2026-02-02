/**
 * Mulberry32 PRNG - fast, simple seeded random number generator
 * Produces deterministic sequences based on seed
 */
export declare class SeededRandom {
    private state;
    constructor(seed: number);
    /**
     * Returns a random 32-bit unsigned integer
     */
    nextUint32(): number;
    /**
     * Fill a buffer with seeded random bytes (uncompressible)
     */
    fillBuffer(buffer: Buffer): void;
}
