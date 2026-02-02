#!/bin/bash
# Mountable cache benchmark - runs on GitHub Actions runner
set -e

BLOB_SAS_URL="$1"
SIZE_GB="${2:-2}"
MOUNT_POINT="${3:-/mnt/cache}"
OVERLAY_TARGET="${4:-./files}"
BLOBFUSE_CACHE_DIR="/tmp/blobfuse-cache"
SQUASHFS_MOUNT="/mnt/squashfs"
OVERLAY_UPPER="/tmp/overlay-upper"
OVERLAY_WORK="/tmp/overlay-work"
LOCAL_SQUASHFS="/tmp/cache.squashfs"

# Timing helper
time_ms() {
    echo $(($(date +%s%3N)))
}

echo "=== Mountable Cache Benchmark ==="
echo ""

# Parse container SAS URL and construct blob URL
# Expected format: https://<account>.blob.core.windows.net/<container>?<sas>
# The SAS must have List permission for blobfuse2 to work
CONTAINER_URL_NO_SAS="${BLOB_SAS_URL%%\?*}"
SAS_TOKEN="${BLOB_SAS_URL#*\?}"

# Extract account name
ACCOUNT=$(echo "$CONTAINER_URL_NO_SAS" | sed -E 's|https://([^.]+)\.blob\.core\.windows\.net/.*|\1|')

# Extract container (everything after the domain, removing any trailing slash)
CONTAINER=$(echo "$CONTAINER_URL_NO_SAS" | sed -E 's|https://[^/]+/||' | sed 's|/$||')

# Construct blob name from size
BLOB_NAME="cache-${SIZE_GB}gb.squashfs"

# Full blob URL for uploads/checks
FULL_BLOB_URL="${CONTAINER_URL_NO_SAS}/${BLOB_NAME}?${SAS_TOKEN}"

echo "Parsed URL:"
echo "  Account: $ACCOUNT"
echo "  Container: $CONTAINER"
echo "  Blob: $BLOB_NAME (constructed from size)"
echo "  SAS length: ${#SAS_TOKEN} chars"
echo "  Size: ${SIZE_GB}GB"
echo ""

# Check if blob exists
echo "Checking if blob exists at: ${CONTAINER_URL_NO_SAS}/${BLOB_NAME}"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -I "${FULL_BLOB_URL}")

if [ "$HTTP_STATUS" == "200" ]; then
    echo "Blob exists, skipping upload."
else
    echo "Blob does not exist (HTTP $HTTP_STATUS), generating and uploading..."
    
    # Install squashfs-tools
    sudo apt-get update && sudo apt-get install -y squashfs-tools

    # Generate files using our TypeScript generator
    echo ""
    echo "=== Generating ${SIZE_GB}GB of test files ==="
    cd $GITHUB_WORKSPACE
    npm ci
    npm run generate -- "$SIZE_GB" "./files"
    
    # Create squashfs
    echo ""
    echo "=== Creating SquashFS image ==="
    mksquashfs "./files" "$LOCAL_SQUASHFS" \
        -noI -noD -noF -noX \
        -no-duplicates \
        -no-sparse \
        -b 1M \
        -processors "$(nproc)"
    
    # Upload to Azure using streaming (PUT with -T)
    echo ""
    echo "=== Uploading to Azure Blob Storage ==="
    SQUASHFS_SIZE=$(stat -c%s "$LOCAL_SQUASHFS")
    echo "File size: $SQUASHFS_SIZE bytes"
    echo "Uploading to: $FULL_BLOB_URL"
    
    curl -X PUT \
        -H "x-ms-blob-type: BlockBlob" \
        -H "Content-Type: application/octet-stream" \
        -T "$LOCAL_SQUASHFS" \
        "${FULL_BLOB_URL}"
    
    echo ""
    echo "Upload complete."
    
    # Clean up local files
    rm -rf "./files" "$LOCAL_SQUASHFS"
fi

# Install blobfuse2 if needed
if ! command -v blobfuse2 &> /dev/null; then
    echo "[$(time_ms)ms] Installing blobfuse2..."
    INSTALL_START=$(time_ms)
    
    sudo wget https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O /tmp/packages-microsoft-prod.deb
    sudo dpkg -i /tmp/packages-microsoft-prod.deb
    sudo apt-get update
    sudo apt-get install -y blobfuse2
    
    INSTALL_END=$(time_ms)
    echo "[${INSTALL_END}ms] blobfuse2 installed in $((INSTALL_END - INSTALL_START))ms"
fi

# Create mount directories
sudo mkdir -p "$MOUNT_POINT" "$SQUASHFS_MOUNT" "$OVERLAY_UPPER" "$OVERLAY_WORK" "$BLOBFUSE_CACHE_DIR" "$OVERLAY_TARGET"

#############################################
# SKIP Benchmark 1 - Streaming mode is too slow with squashfs
# (Each squashfs block read = HTTP roundtrip = ~30s per file)
#############################################
echo "=== SKIPPING Benchmark 1: Streaming mode ==="
echo "Streaming mode (no local cache) is incompatible with squashfs."
echo "squashfs makes many small reads per file, each causing an HTTP roundtrip."
echo ""

STREAMING_MOUNT_TIME=0
STREAMING_FIRST_BYTE=0
STREAMING_HYDRATE=0

#############################################
# Benchmark 2: Mount WITH local caching
#############################################
echo ""
echo "=== Benchmark 2: Cached (local file cache) ==="

MOUNT_START=$(time_ms)

# Create blobfuse2 config with file cache
cat > /tmp/blobfuse-cached.yaml << EOF
allow-other: true
logging:
  type: silent
components:
  - libfuse
  - file_cache
  - azstorage
libfuse:
  attribute-expiration-sec: 120
  entry-expiration-sec: 120
file_cache:
  path: ${BLOBFUSE_CACHE_DIR}
  timeout-sec: 0
  allow-non-empty-temp: true
azstorage:
  type: block
  account-name: ${ACCOUNT}
  container: ${CONTAINER}
  endpoint: https://${ACCOUNT}.blob.core.windows.net
  mode: sas
  sas: ${SAS_TOKEN}
EOF

sudo blobfuse2 mount "$MOUNT_POINT" --config-file=/tmp/blobfuse-cached.yaml --read-only

MOUNT_END=$(time_ms)
MOUNT_TIME=$((MOUNT_END - MOUNT_START))
echo "[${MOUNT_END}ms] blobfuse2 mount complete: ${MOUNT_TIME}ms"

# Mount squashfs
SQUASH_START=$(time_ms)
sudo mount -t squashfs -o ro "$MOUNT_POINT/$BLOB_NAME" "$SQUASHFS_MOUNT"
SQUASH_END=$(time_ms)
SQUASH_TIME=$((SQUASH_END - SQUASH_START))
echo "[${SQUASH_END}ms] squashfs mount complete: ${SQUASH_TIME}ms"

# Mount overlayfs
OVERLAY_START=$(time_ms)
sudo mount -t overlay overlay -o "lowerdir=$SQUASHFS_MOUNT,upperdir=$OVERLAY_UPPER,workdir=$OVERLAY_WORK" "$OVERLAY_TARGET"
OVERLAY_END=$(time_ms)
OVERLAY_TIME=$((OVERLAY_END - OVERLAY_START))
echo "[${OVERLAY_END}ms] overlayfs mount complete: ${OVERLAY_TIME}ms"

TOTAL_MOUNT_TIME=$((MOUNT_TIME + SQUASH_TIME + OVERLAY_TIME))
echo "[${OVERLAY_END}ms] Total mount time (cached): ${TOTAL_MOUNT_TIME}ms"

# Count files and find first file
TOTAL_FILES=$(find "$OVERLAY_TARGET" -type f 2>/dev/null | wc -l)
TOTAL_SIZE_MB=$((TOTAL_FILES * 2))
FIRST_FILE=$(find "$OVERLAY_TARGET" -type f 2>/dev/null | head -1)
echo "Total files: $TOTAL_FILES ($TOTAL_SIZE_MB MB)"
echo "First file: $FIRST_FILE"

# Time to first file
FIRST_FILE_START=$(time_ms)
head -c 1 "$FIRST_FILE" > /dev/null
FIRST_FILE_END=$(time_ms)
FIRST_FILE_TIME=$((FIRST_FILE_END - FIRST_FILE_START))
echo "[${FIRST_FILE_END}ms] Time to first byte: ${FIRST_FILE_TIME}ms"

# Full read (hydration) - cold cache with logging
echo "Hydrating (cold cache)..."
HYDRATE_START=$(time_ms)
FILES_READ=0

echo "[0ms] Beginning file enumeration (cold)..."
FILE_LIST=$(find "$OVERLAY_TARGET" -type f 2>/dev/null)
ENUM_TIME=$(($(time_ms) - HYDRATE_START))
echo "[${ENUM_TIME}ms] File enumeration complete (cold)"

echo "[${ENUM_TIME}ms] Beginning sequential reads (cold)..."
for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    if [ $((FILES_READ % 10)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - HYDRATE_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] Cold read ${FILES_READ}/${TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

HYDRATE_END=$(time_ms)
HYDRATE_TIME=$((HYDRATE_END - HYDRATE_START))
HYDRATE_RATE=$((TOTAL_SIZE_MB * 1000 / (HYDRATE_TIME + 1)))
echo "[${HYDRATE_TIME}ms] Full hydration (cold cache) complete: ${HYDRATE_TIME}ms (${HYDRATE_RATE} MB/s)"

# Full read - warm cache with logging
echo ""
echo "Reading (warm cache)..."
HYDRATE2_START=$(time_ms)
FILES_READ=0

echo "[0ms] Beginning sequential reads (warm)..."
for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    if [ $((FILES_READ % 10)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - HYDRATE2_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] Warm read ${FILES_READ}/${TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

HYDRATE2_END=$(time_ms)
HYDRATE2_TIME=$((HYDRATE2_END - HYDRATE2_START))
HYDRATE2_RATE=$((TOTAL_SIZE_MB * 1000 / (HYDRATE2_TIME + 1)))
echo "[${HYDRATE2_TIME}ms] Full read (warm cache) complete: ${HYDRATE2_TIME}ms (${HYDRATE2_RATE} MB/s)"

echo ""
echo "[$(time_ms)ms] Starting cleanup for cached benchmark..."

# Cleanup
echo "[$(time_ms)ms] Unmounting overlay..."
sudo umount "$OVERLAY_TARGET" || true
echo "[$(time_ms)ms] Unmounting squashfs..."
sudo umount "$SQUASHFS_MOUNT" || true
echo "[$(time_ms)ms] Unmounting blobfuse2..."
sudo blobfuse2 unmount "$MOUNT_POINT" || true
echo "[$(time_ms)ms] Cleanup complete"

CACHED_MOUNT_TIME=$TOTAL_MOUNT_TIME
CACHED_FIRST_BYTE=$FIRST_FILE_TIME
CACHED_HYDRATE_COLD=$HYDRATE_TIME
CACHED_HYDRATE_WARM=$HYDRATE2_TIME

#############################################
# Benchmark 3: Individual files (no squashfs)
# This is the on-demand approach - files are fetched individually
#############################################
echo ""
echo "=== Benchmark 3: Individual Files (on-demand, no squashfs) ==="

# Check if files already uploaded to blob storage
INDIVIDUAL_PREFIX="cache-${SIZE_GB}gb-files"
MARKER_BLOB="${CONTAINER_URL_NO_SAS}/${INDIVIDUAL_PREFIX}/.marker?${SAS_TOKEN}"
MARKER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -I "${MARKER_BLOB}")

if [ "$MARKER_STATUS" == "200" ]; then
    echo "Individual files already uploaded (marker found)"
else
    echo "Individual files not found, uploading..."
    
    # Generate files if not present
    if [ ! -d "$GITHUB_WORKSPACE/files" ]; then
        echo "[$(time_ms)ms] Generating ${SIZE_GB}GB of test files..."
        cd $GITHUB_WORKSPACE
        npm ci 2>/dev/null || true
        npm run generate -- "$SIZE_GB" "./files"
    fi
    
    # Upload files using azcopy (much faster for many files)
    echo "[$(time_ms)ms] Installing azcopy..."
    cd /tmp
    curl -sL https://aka.ms/downloadazcopy-v10-linux | tar xz --strip-components=1
    sudo mv azcopy /usr/local/bin/
    
    echo "[$(time_ms)ms] Uploading individual files to blob storage..."
    UPLOAD_START=$(time_ms)
    
    # Upload entire directory preserving structure
    azcopy copy "$GITHUB_WORKSPACE/files/*" "${CONTAINER_URL_NO_SAS}/${INDIVIDUAL_PREFIX}/?${SAS_TOKEN}" \
        --recursive \
        --put-md5
    
    # Create marker file to indicate upload complete
    echo "uploaded" | curl -X PUT \
        -H "x-ms-blob-type: BlockBlob" \
        -H "Content-Type: text/plain" \
        --data-binary @- \
        "${MARKER_BLOB}"
    
    UPLOAD_END=$(time_ms)
    UPLOAD_TIME=$((UPLOAD_END - UPLOAD_START))
    echo "[${UPLOAD_TIME}ms] Upload complete"
    
    # Cleanup local files
    rm -rf "$GITHUB_WORKSPACE/files"
fi

# Setup for individual files benchmark
INDIVIDUAL_MOUNT="/mnt/individual-cache"
INDIVIDUAL_OVERLAY_TARGET="./files-individual"
INDIVIDUAL_OVERLAY_UPPER="/tmp/individual-overlay-upper"
INDIVIDUAL_OVERLAY_WORK="/tmp/individual-overlay-work"
INDIVIDUAL_BLOBFUSE_CACHE="/tmp/individual-blobfuse-cache"

sudo mkdir -p "$INDIVIDUAL_MOUNT" "$INDIVIDUAL_OVERLAY_TARGET" "$INDIVIDUAL_OVERLAY_UPPER" "$INDIVIDUAL_OVERLAY_WORK" "$INDIVIDUAL_BLOBFUSE_CACHE"

echo ""
echo "--- Individual Files: On-Demand Mode (streaming) ---"

INDIVIDUAL_MOUNT_START=$(time_ms)

# Create blobfuse2 config for individual files - minimal caching for true on-demand
cat > /tmp/blobfuse-individual-streaming.yaml << EOF
allow-other: true
logging:
  type: silent
components:
  - libfuse
  - stream
  - azstorage
libfuse:
  attribute-expiration-sec: 120
  entry-expiration-sec: 120
stream:
  block-size-mb: 4
  buffer-size-mb: 16
  max-buffers: 4
azstorage:
  type: block
  account-name: ${ACCOUNT}
  container: ${CONTAINER}
  endpoint: https://${ACCOUNT}.blob.core.windows.net
  mode: sas
  sas: "${SAS_TOKEN}"
  virtual-directory: true
  subdirectory: ${INDIVIDUAL_PREFIX}
EOF

sudo blobfuse2 mount "$INDIVIDUAL_MOUNT" --config-file=/tmp/blobfuse-individual-streaming.yaml --read-only

INDIVIDUAL_MOUNT_END=$(time_ms)
INDIVIDUAL_MOUNT_TIME=$((INDIVIDUAL_MOUNT_END - INDIVIDUAL_MOUNT_START))
echo "[${INDIVIDUAL_MOUNT_TIME}ms] blobfuse2 mount (individual files) complete: ${INDIVIDUAL_MOUNT_TIME}ms"

# Mount overlayfs (no squashfs needed!)
INDIVIDUAL_OVERLAY_START=$(time_ms)
sudo mount -t overlay overlay -o "lowerdir=$INDIVIDUAL_MOUNT,upperdir=$INDIVIDUAL_OVERLAY_UPPER,workdir=$INDIVIDUAL_OVERLAY_WORK" "$INDIVIDUAL_OVERLAY_TARGET"
INDIVIDUAL_OVERLAY_END=$(time_ms)
INDIVIDUAL_OVERLAY_TIME=$((INDIVIDUAL_OVERLAY_END - INDIVIDUAL_OVERLAY_START))
echo "[${INDIVIDUAL_OVERLAY_TIME}ms] overlayfs mount complete: ${INDIVIDUAL_OVERLAY_TIME}ms"

INDIVIDUAL_TOTAL_MOUNT=$((INDIVIDUAL_MOUNT_TIME + INDIVIDUAL_OVERLAY_TIME))
echo "[${INDIVIDUAL_TOTAL_MOUNT}ms] Total mount time (individual files): ${INDIVIDUAL_TOTAL_MOUNT}ms"

# Time to first file
echo ""
echo "Testing on-demand file access..."
INDIVIDUAL_FIRST_START=$(time_ms)
INDIVIDUAL_FIRST_FILE=$(find "$INDIVIDUAL_OVERLAY_TARGET" -type f 2>/dev/null | head -1)
echo "First file: $INDIVIDUAL_FIRST_FILE"
head -c 1 "$INDIVIDUAL_FIRST_FILE" > /dev/null
INDIVIDUAL_FIRST_END=$(time_ms)
INDIVIDUAL_FIRST_TIME=$((INDIVIDUAL_FIRST_END - INDIVIDUAL_FIRST_START))
echo "[${INDIVIDUAL_FIRST_TIME}ms] Time to first byte (on-demand): ${INDIVIDUAL_FIRST_TIME}ms"

# Read a few files on-demand
echo ""
echo "Reading 10 random files on-demand..."
INDIVIDUAL_SAMPLE_START=$(time_ms)
SAMPLE_FILES=$(find "$INDIVIDUAL_OVERLAY_TARGET" -type f 2>/dev/null | shuf | head -10)
for f in $SAMPLE_FILES; do
    cat "$f" > /dev/null
done
INDIVIDUAL_SAMPLE_END=$(time_ms)
INDIVIDUAL_SAMPLE_TIME=$((INDIVIDUAL_SAMPLE_END - INDIVIDUAL_SAMPLE_START))
echo "[${INDIVIDUAL_SAMPLE_TIME}ms] 10 random files read in ${INDIVIDUAL_SAMPLE_TIME}ms (avg: $((INDIVIDUAL_SAMPLE_TIME / 10))ms per file)"

# Count files for full hydration
INDIVIDUAL_TOTAL_FILES=$(find "$INDIVIDUAL_OVERLAY_TARGET" -type f 2>/dev/null | wc -l)
INDIVIDUAL_TOTAL_SIZE_MB=$((INDIVIDUAL_TOTAL_FILES * 2))
echo "Total files available: $INDIVIDUAL_TOTAL_FILES ($INDIVIDUAL_TOTAL_SIZE_MB MB)"

# Full hydration test
echo ""
echo "Full hydration (all files, on-demand)..."
INDIVIDUAL_HYDRATE_START=$(time_ms)
FILES_READ=0

FILE_LIST=$(find "$INDIVIDUAL_OVERLAY_TARGET" -type f 2>/dev/null)
for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    if [ $((FILES_READ % 50)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - INDIVIDUAL_HYDRATE_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] On-demand read ${FILES_READ}/${INDIVIDUAL_TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

INDIVIDUAL_HYDRATE_END=$(time_ms)
INDIVIDUAL_HYDRATE_TIME=$((INDIVIDUAL_HYDRATE_END - INDIVIDUAL_HYDRATE_START))
INDIVIDUAL_HYDRATE_RATE=$((INDIVIDUAL_TOTAL_SIZE_MB * 1000 / (INDIVIDUAL_HYDRATE_TIME + 1)))
echo "[${INDIVIDUAL_HYDRATE_TIME}ms] Full hydration (individual files): ${INDIVIDUAL_HYDRATE_TIME}ms (${INDIVIDUAL_HYDRATE_RATE} MB/s)"

# Cleanup
echo ""
echo "[$(time_ms)ms] Cleaning up individual files benchmark..."
sudo umount "$INDIVIDUAL_OVERLAY_TARGET" || true
sudo blobfuse2 unmount "$INDIVIDUAL_MOUNT" || true
echo "[$(time_ms)ms] Cleanup complete"

#############################################
# Output results
#############################################
echo ""
echo "=========================================="
echo "        MOUNTABLE CACHE BENCHMARK"
echo "=========================================="
echo ""
echo "SQUASHFS + CACHED MODE (blobfuse2 file cache):"
echo "  Total mount time:      ${CACHED_MOUNT_TIME}ms"
echo "  Time to first byte:    ${CACHED_FIRST_BYTE}ms"
echo "  Full hydration (cold): ${CACHED_HYDRATE_COLD}ms"
echo "  Full read (warm):      ${CACHED_HYDRATE_WARM}ms"
echo ""
echo "INDIVIDUAL FILES (on-demand, no squashfs):"
echo "  Total mount time:      ${INDIVIDUAL_TOTAL_MOUNT}ms"
echo "  Time to first byte:    ${INDIVIDUAL_FIRST_TIME}ms"
echo "  10 random files:       ${INDIVIDUAL_SAMPLE_TIME}ms (avg: $((INDIVIDUAL_SAMPLE_TIME / 10))ms)"
echo "  Full hydration:        ${INDIVIDUAL_HYDRATE_TIME}ms (${INDIVIDUAL_HYDRATE_RATE} MB/s)"
echo ""
echo "=========================================="

# Output as GitHub Actions step outputs
echo "cached_mount_time=${CACHED_MOUNT_TIME}" >> $GITHUB_OUTPUT
echo "cached_first_byte=${CACHED_FIRST_BYTE}" >> $GITHUB_OUTPUT
echo "cached_hydrate_cold=${CACHED_HYDRATE_COLD}" >> $GITHUB_OUTPUT
echo "cached_hydrate_warm=${CACHED_HYDRATE_WARM}" >> $GITHUB_OUTPUT
echo "individual_mount_time=${INDIVIDUAL_TOTAL_MOUNT}" >> $GITHUB_OUTPUT
echo "individual_first_byte=${INDIVIDUAL_FIRST_TIME}" >> $GITHUB_OUTPUT
echo "individual_sample_time=${INDIVIDUAL_SAMPLE_TIME}" >> $GITHUB_OUTPUT
echo "individual_hydrate_time=${INDIVIDUAL_HYDRATE_TIME}" >> $GITHUB_OUTPUT
