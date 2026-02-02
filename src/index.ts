import * as core from '@actions/core';
import * as cache from '@actions/cache';
import * as path from 'path';
import * as fs from 'fs';
import { generateFileHierarchy, deleteFileHierarchy, verifyFileHierarchy } from './generate';

// Possible tmpfs locations on different systems
const TMPFS_CANDIDATES = ['/mnt/tmpfs', '/tmpfs', '/dev/shm'];

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
      
      core.info('=== TMPFS MODE ENABLED ===');
      core.info(`Detected tmpfs at: ${tmpfsDir}`);
      core.info(`Using ${tmpfsDir}/benchmark-files for files`);
      core.info(`Using ${tmpfsRunnerTemp} for cache archive`);
      
      // Generate files in tmpfs
      filesPath = path.join(tmpfsDir, 'benchmark-files');
      
      // Create RUNNER_TEMP in tmpfs and override the environment variable
      // This makes @actions/cache create/extract archives in tmpfs
      if (!fs.existsSync(tmpfsRunnerTemp)) {
        fs.mkdirSync(tmpfsRunnerTemp, { recursive: true });
      }
      originalRunnerTemp = process.env['RUNNER_TEMP'];
      process.env['RUNNER_TEMP'] = tmpfsRunnerTemp;
      core.info(`RUNNER_TEMP overridden: ${originalRunnerTemp} -> ${tmpfsRunnerTemp}`);
    } else {
      const workDir = process.cwd();
      filesPath = path.join(workDir, 'files');
    }

    // Use a different cache key for tmpfs to avoid conflicts
    const cacheKey = useTmpfs 
      ? `benchmark-cache-tmpfs-${sizeGb}gb-v1`
      : `benchmark-cache-${sizeGb}gb-v1`;
    
    core.info(`Configured for ${sizeGb}GB, cache key: ${cacheKey}`);
    core.info(`Files path: ${filesPath}`);

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
    const cacheId = await cache.saveCache([filesPath], cacheKey);
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
    const restoredKey = await cache.restoreCache([filesPath], cacheKey);
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
