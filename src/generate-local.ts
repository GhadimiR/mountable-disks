import * as path from 'path';
import { generateFileHierarchy } from './generate';

const SIZE_GB = parseInt(process.argv[2] || '2', 10);
const OUTPUT_DIR = process.argv[3] || 'files';

async function main(): Promise<void> {
  const filesPath = path.resolve(process.cwd(), OUTPUT_DIR);
  
  console.log(`Generating ${SIZE_GB}GB of uncompressible data in ${filesPath}`);
  console.log('');
  
  const start = Date.now();
  await generateFileHierarchy(filesPath, SIZE_GB);
  const elapsed = Date.now() - start;
  
  console.log('');
  console.log(`Done in ${elapsed}ms`);
}

main().catch((err) => {
  console.error('Error:', err);
  process.exit(1);
});
