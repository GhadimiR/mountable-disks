#!/bin/bash
set -e

SIZE_GB="${1:-2}"
OUTPUT_DIR="${2:-./files}"
SQUASHFS_OUTPUT="${3:-cache.squashfs}"

echo "=== Mountable Cache Image Generator ==="
echo "Size: ${SIZE_GB}GB"
echo "Output dir: ${OUTPUT_DIR}"
echo "SquashFS output: ${SQUASHFS_OUTPUT}"
echo ""

# Check for mksquashfs
if ! command -v mksquashfs &> /dev/null; then
    echo "Error: mksquashfs not found. Install with:"
    echo "  macOS: brew install squashfs"
    echo "  Ubuntu: sudo apt-get install squashfs-tools"
    exit 1
fi

# Generate the files
echo "=== Step 1: Generating ${SIZE_GB}GB of files ==="
npm run generate -- "$SIZE_GB" "$OUTPUT_DIR"

# Create SquashFS image (no compression since data is random)
echo ""
echo "=== Step 2: Creating SquashFS image ==="
echo "Using no compression (-noI -noD -noF -noX) since data is uncompressible..."
START_TIME=$(date +%s%3N)

mksquashfs "$OUTPUT_DIR" "$SQUASHFS_OUTPUT" \
    -noI -noD -noF -noX \
    -no-duplicates \
    -no-sparse \
    -b 1M \
    -processors "$(nproc 2>/dev/null || sysctl -n hw.ncpu)"

END_TIME=$(date +%s%3N)
ELAPSED=$((END_TIME - START_TIME))

SQUASHFS_SIZE=$(stat -f%z "$SQUASHFS_OUTPUT" 2>/dev/null || stat -c%s "$SQUASHFS_OUTPUT")
SQUASHFS_SIZE_GB=$(echo "scale=2; $SQUASHFS_SIZE / 1024 / 1024 / 1024" | bc)

echo ""
echo "=== Complete ==="
echo "SquashFS image: $SQUASHFS_OUTPUT"
echo "Image size: ${SQUASHFS_SIZE_GB}GB"
echo "Creation time: ${ELAPSED}ms"
echo ""
echo "Upload this file to Azure Blob Storage and create a SAS URL."
echo "Store the SAS URL as a repo secret named: CACHE_BLOB_SAS_URL"
