import * as fs from 'fs';
import * as path from 'path';
import { execSync } from 'child_process';
import { generateFileHierarchy, deleteFileHierarchy, verifyFileHierarchy } from './generate';

const FILES_DIR = 'files';
const CACHE_FILE = 'cache-benchmark.tar.zst';
const SIZE_GB = 8; // Default size, can be changed for testing

function log(msg: string): void {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

async function run(): Promise<void> {
  const workDir = process.cwd();
  const filesPath = path.join(workDir, FILES_DIR);
  const cachePath = path.join(workDir, CACHE_FILE);

  // Clean up any previous runs
  if (fs.existsSync(filesPath)) {
    log('Cleaning up previous files directory...');
    fs.rmSync(filesPath, { recursive: true, force: true });
  }
  if (fs.existsSync(cachePath)) {
    log('Cleaning up previous cache file...');
    fs.unlinkSync(cachePath);
  }

  // Step 1: Generate the file hierarchy
  log(`=== Step 1: Generate ${SIZE_GB}GB file hierarchy ===`);
  const genStart = Date.now();
  await generateFileHierarchy(filesPath, SIZE_GB);
  const genTime = (Date.now() - genStart) / 1000;
  log(`Generation took ${genTime.toFixed(1)}s`);

  // Step 2: Save to "cache" (tar with zstd compression - same as GitHub)
  log('=== Step 2: Create cache archive (tar + zstd) ===');
  const saveStart = Date.now();
  execSync(`tar --use-compress-program=zstd -cf "${cachePath}" -C "${workDir}" "${FILES_DIR}"`, {
    stdio: 'inherit',
  });
  const saveTime = (Date.now() - saveStart) / 1000;
  const cacheSize = fs.statSync(cachePath).size;
  const cacheSizeGB = cacheSize / (1024 * 1024 * 1024);
  log(`Cache save took ${saveTime.toFixed(1)}s (archive size: ${cacheSizeGB.toFixed(2)} GB)`);

  // Step 3: Delete the directory
  log('=== Step 3: Delete file hierarchy ===');
  const deleteStart = Date.now();
  await deleteFileHierarchy(filesPath);
  const deleteTime = (Date.now() - deleteStart) / 1000;
  log(`Deletion took ${deleteTime.toFixed(1)}s`);

  // Step 4: Restore from "cache"
  log('=== Step 4: Restore from cache archive ===');
  const restoreStart = Date.now();
  execSync(`tar --use-compress-program=zstd -xf "${cachePath}" -C "${workDir}"`, {
    stdio: 'inherit',
  });
  const restoreTime = (Date.now() - restoreStart) / 1000;
  log(`Cache restore took ${restoreTime.toFixed(1)}s`);

  // Step 5: Verify restoration
  log('=== Step 5: Verify restored data ===');
  const verifyStart = Date.now();
  const verified = await verifyFileHierarchy(filesPath, SIZE_GB);
  const verifyTime = (Date.now() - verifyStart) / 1000;
  if (!verified) {
    throw new Error('Verification failed!');
  }
  log(`Verification took ${verifyTime.toFixed(1)}s`);

  // Summary
  console.log('');
  console.log('========================================');
  console.log('         BENCHMARK SUMMARY');
  console.log('========================================');
  console.log(`Generation time:    ${genTime.toFixed(1)}s`);
  console.log(`Cache save time:    ${saveTime.toFixed(1)}s`);
  console.log(`Cache archive size: ${cacheSizeGB.toFixed(2)} GB`);
  console.log(`Deletion time:      ${deleteTime.toFixed(1)}s`);
  console.log(`Cache restore time: ${restoreTime.toFixed(1)}s`);
  console.log(`Verification time:  ${verifyTime.toFixed(1)}s`);
  console.log('----------------------------------------');
  console.log(`Total time:         ${(genTime + saveTime + deleteTime + restoreTime + verifyTime).toFixed(1)}s`);
  console.log('========================================');

  // Cleanup
  log('Cleaning up...');
  fs.rmSync(filesPath, { recursive: true, force: true });
  fs.unlinkSync(cachePath);
  log('Done!');
}

run().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
