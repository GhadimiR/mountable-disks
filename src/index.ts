import * as core from '@actions/core';
import * as cache from '@actions/cache';
import * as path from 'path';
import { generateFileHierarchy, deleteFileHierarchy, verifyFileHierarchy } from './generate';

const CACHE_KEY = 'benchmark-cache-8gb-v1';
const FILES_DIR = 'files';

async function run(): Promise<void> {
  try {
    const workDir = process.cwd();
    const filesPath = path.join(workDir, FILES_DIR);

    // Step 1: Generate the file hierarchy
    core.startGroup('Step 1: Generate 8GB file hierarchy');
    const genStart = Date.now();
    await generateFileHierarchy(filesPath);
    const genTimeMs = Date.now() - genStart;
    core.info(`[${genTimeMs}ms] Generation complete`);
    core.endGroup();

    // Step 2: Save to cache
    core.startGroup('Step 2: Save to cache');
    const saveStart = Date.now();
    const cacheId = await cache.saveCache([filesPath], CACHE_KEY);
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
    const restoredKey = await cache.restoreCache([filesPath], CACHE_KEY);
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
    const verified = await verifyFileHierarchy(filesPath);
    const verifyTimeMs = Date.now() - verifyStart;
    if (!verified) {
      throw new Error('Verification failed - restored data does not match expected structure');
    }
    core.info(`[${verifyTimeMs}ms] Verification complete`);
    core.endGroup();

    // Summary
    core.info('');
    core.info('=== Benchmark Summary ===');
    core.info(`Generation time:    ${genTimeMs}ms`);
    core.info(`Cache save time:    ${saveTimeMs}ms`);
    core.info(`Deletion time:      ${deleteTimeMs}ms`);
    core.info(`Cache restore time: ${restoreTimeMs}ms`);
    core.info(`Verification time:  ${verifyTimeMs}ms`);
    core.info(`Total time:         ${genTimeMs + saveTimeMs + deleteTimeMs + restoreTimeMs + verifyTimeMs}ms`);
    core.info('=========================');

  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(error.message);
    } else {
      core.setFailed('An unexpected error occurred');
    }
  }
}

run();
