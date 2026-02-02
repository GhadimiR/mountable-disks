/**
 * Generate the complete file hierarchy with uncompressible data
 */
export declare function generateFileHierarchy(baseDir: string, sizeGb: number): Promise<void>;
/**
 * Delete the file hierarchy
 */
export declare function deleteFileHierarchy(baseDir: string): Promise<void>;
/**
 * Verify the file hierarchy exists and has correct structure
 */
export declare function verifyFileHierarchy(baseDir: string, sizeGb: number): Promise<boolean>;
