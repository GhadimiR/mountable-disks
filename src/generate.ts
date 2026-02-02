import * as fs from 'fs';
import * as path from 'path';
import * as core from '@actions/core';
import { SeededRandom } from './seeded-random';

// Configuration for N GB total:
// N level-1 dirs × 10 level-2 dirs × 10 level-3 dirs × 5 files × 2MB = N GB
const LEVEL_2_DIRS = 10;
const LEVEL_3_DIRS = 10;
const FILES_PER_LEAF = 5;
const FILE_SIZE_BYTES = 2 * 1024 * 1024; // 2MB per file
const MASTER_SEED = 0xdeadbeef;

// Write buffer size for streaming writes (64KB chunks)
const WRITE_CHUNK_SIZE = 64 * 1024;

/**
 * Generate a deterministic seed for a specific file based on its path indices
 */
function getFileSeed(l1: number, l2: number, l3: number, fileNum: number): number {
  // Combine indices into a unique seed for each file
  return MASTER_SEED ^ (l1 << 24) ^ (l2 << 16) ^ (l3 << 8) ^ fileNum;
}

/**
 * Write a single file with seeded random (uncompressible) data
 */
async function writeRandomFile(
  filePath: string,
  seed: number,
  size: number
): Promise<void> {
  const rng = new SeededRandom(seed);
  const chunk = Buffer.allocUnsafe(WRITE_CHUNK_SIZE);
  
  const fd = fs.openSync(filePath, 'w');
  try {
    let bytesWritten = 0;
    while (bytesWritten < size) {
      const bytesToWrite = Math.min(WRITE_CHUNK_SIZE, size - bytesWritten);
      const writeBuffer = bytesToWrite === WRITE_CHUNK_SIZE ? chunk : chunk.subarray(0, bytesToWrite);
      rng.fillBuffer(writeBuffer);
      fs.writeSync(fd, writeBuffer);
      bytesWritten += bytesToWrite;
    }
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Generate the complete file hierarchy with uncompressible data
 */
export async function generateFileHierarchy(baseDir: string, sizeGb: number): Promise<void> {
  const level1Dirs = sizeGb; // 1 dir per GB
  const totalFiles = level1Dirs * LEVEL_2_DIRS * LEVEL_3_DIRS * FILES_PER_LEAF;
  const totalSizeGB = (totalFiles * FILE_SIZE_BYTES) / (1024 * 1024 * 1024);
  
  core.info(`Generating file hierarchy in ${baseDir}`);
  core.info(`Structure: ${level1Dirs} × ${LEVEL_2_DIRS} × ${LEVEL_3_DIRS} × ${FILES_PER_LEAF} files`);
  core.info(`Total: ${totalFiles} files, ${totalSizeGB.toFixed(2)} GB`);

  // Create base directory
  fs.mkdirSync(baseDir, { recursive: true });

  let filesCreated = 0;
  const startTime = Date.now();

  for (let l1 = 0; l1 < level1Dirs; l1++) {
    const l1Dir = path.join(baseDir, `dir_${l1}`);
    
    for (let l2 = 0; l2 < LEVEL_2_DIRS; l2++) {
      const l2Dir = path.join(l1Dir, `sub_${l2}`);
      
      for (let l3 = 0; l3 < LEVEL_3_DIRS; l3++) {
        const l3Dir = path.join(l2Dir, `leaf_${l3}`);
        fs.mkdirSync(l3Dir, { recursive: true });

        for (let f = 0; f < FILES_PER_LEAF; f++) {
          const filePath = path.join(l3Dir, `data_${f}.bin`);
          const seed = getFileSeed(l1, l2, l3, f);
          await writeRandomFile(filePath, seed, FILE_SIZE_BYTES);
          filesCreated++;

          // Progress logging every 100 files
          if (filesCreated % 100 === 0) {
            const elapsedMs = Date.now() - startTime;
            const rate = filesCreated / elapsedMs;
            const remainingMs = (totalFiles - filesCreated) / rate;
            core.info(`[${elapsedMs}ms] Progress: ${filesCreated}/${totalFiles} files (${((filesCreated / totalFiles) * 100).toFixed(1)}%) - ETA: ${remainingMs.toFixed(0)}ms`);
          }
        }
      }
    }
  }

  const totalElapsedMs = Date.now() - startTime;
  const throughputMBs = (totalSizeGB * 1024) / (totalElapsedMs / 1000);
  core.info(`[${totalElapsedMs}ms] Generation complete: ${totalFiles} files (${throughputMBs.toFixed(1)} MB/s)`);
}

/**
 * Delete the file hierarchy
 */
export async function deleteFileHierarchy(baseDir: string): Promise<void> {
  core.info(`Deleting file hierarchy at ${baseDir}`);
  const startTime = Date.now();
  
  fs.rmSync(baseDir, { recursive: true, force: true });
  
  const elapsedMs = Date.now() - startTime;
  core.info(`[${elapsedMs}ms] Deletion complete`);
}

/**
 * Verify the file hierarchy exists and has correct structure
 */
export async function verifyFileHierarchy(baseDir: string, sizeGb: number): Promise<boolean> {
  core.info(`Verifying file hierarchy at ${baseDir}`);
  
  const level1Dirs = sizeGb; // 1 dir per GB
  let filesVerified = 0;
  let totalSize = 0;

  for (let l1 = 0; l1 < level1Dirs; l1++) {
    for (let l2 = 0; l2 < LEVEL_2_DIRS; l2++) {
      for (let l3 = 0; l3 < LEVEL_3_DIRS; l3++) {
        for (let f = 0; f < FILES_PER_LEAF; f++) {
          const filePath = path.join(
            baseDir,
            `dir_${l1}`,
            `sub_${l2}`,
            `leaf_${l3}`,
            `data_${f}.bin`
          );
          
          if (!fs.existsSync(filePath)) {
            core.error(`Missing file: ${filePath}`);
            return false;
          }
          
          const stats = fs.statSync(filePath);
          if (stats.size !== FILE_SIZE_BYTES) {
            core.error(`Wrong size for ${filePath}: expected ${FILE_SIZE_BYTES}, got ${stats.size}`);
            return false;
          }
          
          filesVerified++;
          totalSize += stats.size;
        }
      }
    }
  }

  const totalSizeGB = totalSize / (1024 * 1024 * 1024);
  core.info(`Verification passed: ${filesVerified} files, ${totalSizeGB.toFixed(2)} GB`);
  return true;
}
