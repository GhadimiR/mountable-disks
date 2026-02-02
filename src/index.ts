import * as core from '@actions/core';
import * as cache from '@actions/cache';
import * as path from 'path';
import * as fs from 'fs';
import { generateFileHierarchy, deleteFileHierarchy, verifyFileHierarchy } from './generate';

// Possible tmpfs locations on different systems
const TMPFS_CANDIDATES = ['/mnt/tmpfs', '/tmpfs', '/dev/shm'];

const SAVE_RETRY_ATTEMPTS = 3;
const RESTORE_RETRY_ATTEMPTS = 6;
const RETRY_BASE_DELAY_MS = 5000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function saveCacheWithRetry(paths: string[], key: string): Promise<number> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= SAVE_RETRY_ATTEMPTS; attempt++) {
    try {
      core.info(`Cache save attempt ${attempt}/${SAVE_RETRY_ATTEMPTS}...`);
      const cacheId = await cache.saveCache(paths, key);
      if (cacheId === -1) {
        throw new Error('Cache save returned -1 (cache not saved)');
      }
      return cacheId;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < SAVE_RETRY_ATTEMPTS) {
        const delay = RETRY_BASE_DELAY_MS * attempt;
        core.warning(`Cache save attempt ${attempt} failed: ${lastError.message}. Retrying in ${delay}ms.`);
        await sleep(delay);
      }
    }
  }

  throw lastError ?? new Error('Cache save failed after retries');
}

async function restoreCacheWithRetry(paths: string[], key: string): Promise<string> {
  let lastError: Error | undefined;

  for (let attempt = 1; attempt <= RESTORE_RETRY_ATTEMPTS; attempt++) {
    try {
      core.info(`Cache restore attempt ${attempt}/${RESTORE_RETRY_ATTEMPTS}...`);
      const restoredKey = await cache.restoreCache(paths, key);
      if (!restoredKey) {
        throw new Error('Cache restore returned undefined');
      }
      return restoredKey;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      if (attempt < RESTORE_RETRY_ATTEMPTS) {
        const delay = RETRY_BASE_DELAY_MS * attempt;
        core.warning(`Cache restore attempt ${attempt} failed: ${lastError.message}. Retrying in ${delay}ms.`);
        await sleep(delay);
      }
    }
  }

  throw lastError ?? new Error('Cache restore failed after retries');
}

function findTmpfsLocation(): string {
  for (const candidate of TMPFS_CANDIDATES) {
    if (fs.existsSync(candidate)) {
      // Verify it's actually a tmpfs by checking if it's writable
      try {
        const testFile = path.join(candidate, '.tmpfs-test');
        fs.writeFileSync(testFile, 'test');
        fs.unlinkSync(testFile);
        return candidate;
      } catch {
        core.debug(`${candidate} exists but is not writable`);
      }
    }
  }
  throw new Error(`No tmpfs location found. Tried: ${TMPFS_CANDIDATES.join(', ')}`);
}

async function run(): Promise<void> {
  try {
    // Read inputs
    const sizeGb = parseInt(core.getInput('size-gb') || '8', 10);
    if (sizeGb < 1 || sizeGb > 10) {
      throw new Error('size-gb must be between 1 and 10');
    }

    const useTmpfs = core.getInput('use-tmpfs') === 'true';
    
    // Determine paths based on tmpfs setting
    let filesPath: string;
    let originalRunnerTemp: string | undefined;

    if (useTmpfs) {
      const tmpfsDir = findTmpfsLocation();
      const tmpfsRunnerTemp = path.join(tmpfsDir, 'runner-temp');
      const tmpfsFilesRoot = path.join(tmpfsDir, 'cache-benchmark');
      
      core.info('=== TMPFS MODE ENABLED ===');
      core.info(`Detected tmpfs at: ${tmpfsDir}`);
      core.info(`Archive temp dir (RUNNER_TEMP): ${tmpfsRunnerTemp}`);
      core.info(`Files dir (tmpfs): ${tmpfsFilesRoot}`);
      core.info('Files and archive I/O in tmpfs - testing upload/download from memory');
      
      // Create RUNNER_TEMP in tmpfs for archive operations
      if (!fs.existsSync(tmpfsRunnerTemp)) {
        fs.mkdirSync(tmpfsRunnerTemp, { recursive: true });
      }
      if (!fs.existsSync(tmpfsFilesRoot)) {
        fs.mkdirSync(tmpfsFilesRoot, { recursive: true });
      }
      originalRunnerTemp = process.env['RUNNER_TEMP'];
      process.env['RUNNER_TEMP'] = tmpfsRunnerTemp;
      core.info(`RUNNER_TEMP overridden: ${originalRunnerTemp} -> ${tmpfsRunnerTemp}`);
      
      filesPath = path.join(tmpfsFilesRoot, 'files');
    } else {
      const workDir = process.cwd();
      filesPath = path.join(workDir, 'files');
    }

    // Use unique key per workflow run to ensure fresh save/restore
    // GITHUB_RUN_ID + GITHUB_RUN_ATTEMPT ensures uniqueness even for re-runs
    const runId = process.env['GITHUB_RUN_ID'];
    const runAttempt = process.env['GITHUB_RUN_ATTEMPT'] || '1';
    const uniqueKey = runId ? `${runId}-${runAttempt}` : `local-${Date.now()}`;
    
    const cacheKey = useTmpfs 
      ? `tmpfs-${sizeGb}gb-${uniqueKey}`
      : `disk-${sizeGb}gb-${uniqueKey}`;
    
    core.info('=== Configuration ===');
    core.info(`Size: ${sizeGb}GB`);
    core.info(`Mode: ${useTmpfs ? 'TMPFS' : 'DISK'}`);
    core.info(`Cache key: ${cacheKey}`);
    core.info(`Files path: ${filesPath}`);
    core.info(`RUNNER_TEMP: ${process.env['RUNNER_TEMP']}`);
    core.info(`GITHUB_RUN_ID: ${process.env['GITHUB_RUN_ID']}`);
    core.info(`GITHUB_RUN_ATTEMPT: ${process.env['GITHUB_RUN_ATTEMPT']}`);
    core.info(`GITHUB_WORKSPACE: ${process.env['GITHUB_WORKSPACE']}`);
    core.info(`cwd: ${process.cwd()}`);
    core.info('====================');

    // Step 1: Generate the file hierarchy
    core.startGroup(`Step 1: Generate ${sizeGb}GB file hierarchy`);
    const genStart = Date.now();
    await generateFileHierarchy(filesPath, sizeGb);
    const genTimeMs = Date.now() - genStart;
    core.info(`[${genTimeMs}ms] Generation complete`);
    core.endGroup();

    // Step 2: Save to cache
    core.startGroup('Step 2: Save to cache');
    const saveStart = Date.now();
    const cacheId = await saveCacheWithRetry([filesPath], cacheKey);
    const saveTimeMs = Date.now() - saveStart;
    if (cacheId === -1) {
      core.warning('Cache save returned -1 (cache may already exist)');
    } else {
      core.info(`Cache saved with ID: ${cacheId}`);
    }
    core.info(`[${saveTimeMs}ms] Cache save complete`);
    core.endGroup();

    // Step 3: Delete the directory
    core.startGroup('Step 3: Delete file hierarchy');
    const deleteStart = Date.now();
    await deleteFileHierarchy(filesPath);
    const deleteTimeMs = Date.now() - deleteStart;
    core.info(`[${deleteTimeMs}ms] Deletion complete`);
    core.endGroup();

    // Step 4: Restore from cache
    core.startGroup('Step 4: Restore from cache');
    const restoreStart = Date.now();
    const restoredKey = await restoreCacheWithRetry([filesPath], cacheKey);
    const restoreTimeMs = Date.now() - restoreStart;
    if (!restoredKey) {
      throw new Error('Cache restore failed - no matching cache found');
    }
    core.info(`Cache restored with key: ${restoredKey}`);
    core.info(`[${restoreTimeMs}ms] Cache restore complete`);
    core.endGroup();

    // Step 5: Verify restoration
    core.startGroup('Step 5: Verify restored data');
    const verifyStart = Date.now();
    const verified = await verifyFileHierarchy(filesPath, sizeGb);
    const verifyTimeMs = Date.now() - verifyStart;
    if (!verified) {
      throw new Error('Verification failed - restored data does not match expected structure');
    }
    core.info(`[${verifyTimeMs}ms] Verification complete`);
    core.endGroup();

    // Summary
    core.info('');
    core.info('=== Benchmark Summary ===');
    core.info(`Mode:               ${useTmpfs ? 'TMPFS' : 'DISK'}`);
    core.info(`Generation time:    ${genTimeMs}ms`);
    core.info(`Cache save time:    ${saveTimeMs}ms`);
    core.info(`Deletion time:      ${deleteTimeMs}ms`);
    core.info(`Cache restore time: ${restoreTimeMs}ms`);
    core.info(`Verification time:  ${verifyTimeMs}ms`);
    core.info(`Total time:         ${genTimeMs + saveTimeMs + deleteTimeMs + restoreTimeMs + verifyTimeMs}ms`);
    core.info('=========================');

    // Set outputs for workflow
    core.setOutput('generate_time', genTimeMs);
    core.setOutput('save_time', saveTimeMs);
    core.setOutput('delete_time', deleteTimeMs);
    core.setOutput('restore_time', restoreTimeMs);
    core.setOutput('verify_time', verifyTimeMs);
    core.setOutput('total_time', genTimeMs + saveTimeMs + deleteTimeMs + restoreTimeMs + verifyTimeMs);

    // Cleanup: restore RUNNER_TEMP if we modified it
    if (useTmpfs && originalRunnerTemp !== undefined) {
      process.env['RUNNER_TEMP'] = originalRunnerTemp;
      core.info(`RUNNER_TEMP restored to: ${originalRunnerTemp}`);
    }

  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(error.message);
    } else {
      core.setFailed('An unexpected error occurred');
    }
  }
}

run();
