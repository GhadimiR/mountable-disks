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

# Check if files already uploaded to blob storage by listing the container
INDIVIDUAL_PREFIX="cache-${SIZE_GB}gb-files"
echo "Checking for individual files at prefix: ${INDIVIDUAL_PREFIX}"

# Use Azure REST API to list blobs with the prefix (skip the marker check)
LIST_URL="${CONTAINER_URL_NO_SAS}?restype=container&comp=list&prefix=${INDIVIDUAL_PREFIX}/dir_&${SAS_TOKEN}"
BLOB_COUNT=$(curl -s "$LIST_URL" | grep -c "<Name>" || echo "0")
echo "Found $BLOB_COUNT blobs with prefix ${INDIVIDUAL_PREFIX}/dir_"

if [ "$BLOB_COUNT" -gt 10 ]; then
    echo "Individual files already uploaded ($BLOB_COUNT blobs found)"
else
    echo "Individual files not found or incomplete, uploading..."
    
    # Generate files - always regenerate since squashfs step may have deleted them
    INDIVIDUAL_FILES_DIR="$GITHUB_WORKSPACE/files-individual-upload"
    echo "[$(time_ms)ms] Generating ${SIZE_GB}GB of test files for individual upload..."
    cd $GITHUB_WORKSPACE
    npm ci 2>/dev/null || true
    rm -rf "$INDIVIDUAL_FILES_DIR"  # Clean any partial data
    npm run generate -- "$SIZE_GB" "$INDIVIDUAL_FILES_DIR"
    
    # Verify files were generated
    GENERATED_COUNT=$(find "$INDIVIDUAL_FILES_DIR" -type f 2>/dev/null | wc -l)
    echo "[$(time_ms)ms] Generated $GENERATED_COUNT files"
    
    if [ "$GENERATED_COUNT" -eq 0 ]; then
        echo "ERROR: No files generated!"
        exit 1
    fi
    
    # Upload files using azcopy (much faster for many files)
    echo "[$(time_ms)ms] Installing azcopy..."
    cd /tmp
    curl -sL https://aka.ms/downloadazcopy-v10-linux | tar xz --strip-components=1
    sudo mv azcopy /usr/local/bin/
    cd $GITHUB_WORKSPACE
    
    echo "[$(time_ms)ms] Uploading individual files to blob storage..."
    echo "Source: $INDIVIDUAL_FILES_DIR"
    echo "Destination: ${CONTAINER_URL_NO_SAS}/${INDIVIDUAL_PREFIX}/"
    UPLOAD_START=$(time_ms)
    
    # Upload entire directory preserving structure
    azcopy copy "$INDIVIDUAL_FILES_DIR/*" "${CONTAINER_URL_NO_SAS}/${INDIVIDUAL_PREFIX}/?${SAS_TOKEN}" \
        --recursive \
        --put-md5 \
        --log-level=WARNING
    
    UPLOAD_END=$(time_ms)
    UPLOAD_TIME=$((UPLOAD_END - UPLOAD_START))
    echo "[${UPLOAD_TIME}ms] Upload complete"
    
    # Cleanup local files
    rm -rf "$INDIVIDUAL_FILES_DIR"
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
# Benchmark 4: JuiceFS (intelligent caching + prefetch)
#############################################
echo ""
echo "=== Benchmark 4: JuiceFS (smart prefetch + block cache) ==="

JUICEFS_MOUNT="/mnt/juicefs"
JUICEFS_OVERLAY_TARGET="./files-juicefs"
JUICEFS_OVERLAY_UPPER="/tmp/juicefs-overlay-upper"
JUICEFS_OVERLAY_WORK="/tmp/juicefs-overlay-work"
JUICEFS_CACHE_DIR="/tmp/juicefs-cache"
JUICEFS_NAME="benchmark-${SIZE_GB}gb"

sudo mkdir -p "$JUICEFS_MOUNT" "$JUICEFS_OVERLAY_TARGET" "$JUICEFS_OVERLAY_UPPER" "$JUICEFS_OVERLAY_WORK" "$JUICEFS_CACHE_DIR"

# Install JuiceFS
echo "[$(time_ms)ms] Installing JuiceFS..."
JUICEFS_INSTALL_START=$(time_ms)
curl -sSL https://d.juicefs.com/install | sh -
JUICEFS_INSTALL_END=$(time_ms)
echo "[$(time_ms)ms] JuiceFS installed in $((JUICEFS_INSTALL_END - JUICEFS_INSTALL_START))ms"

# Start Redis for metadata
echo "[$(time_ms)ms] Starting Redis container..."
docker run -d --name redis-juicefs -p 6379:6379 redis:alpine
sleep 2  # Wait for Redis to start

# Check if JuiceFS filesystem exists (format if not)
JUICEFS_META_URL="redis://localhost:6379/1"

echo "[$(time_ms)ms] Checking JuiceFS filesystem..."
if ! juicefs status "$JUICEFS_META_URL" 2>/dev/null; then
    echo "[$(time_ms)ms] Formatting JuiceFS filesystem..."
    juicefs format \
        --storage azblob \
        --bucket "https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}" \
        --access-key "${ACCOUNT}" \
        --secret-key "${SAS_TOKEN}" \
        --block-size 4096 \
        --compress none \
        "$JUICEFS_META_URL" \
        "$JUICEFS_NAME"
fi

# Check if files exist in JuiceFS
echo "[$(time_ms)ms] Mounting JuiceFS to check for files..."
juicefs mount "$JUICEFS_META_URL" "$JUICEFS_MOUNT" \
    --cache-dir "$JUICEFS_CACHE_DIR" \
    --cache-size 4096 \
    --prefetch 3 \
    --buffer-size 300 \
    -d

sleep 1
JUICEFS_FILE_COUNT=$(find "$JUICEFS_MOUNT" -type f 2>/dev/null | wc -l)
echo "[$(time_ms)ms] Found $JUICEFS_FILE_COUNT files in JuiceFS"

if [ "$JUICEFS_FILE_COUNT" -lt 10 ]; then
    echo "[$(time_ms)ms] Uploading files to JuiceFS..."
    
    # Generate files if needed
    JUICEFS_FILES_DIR="$GITHUB_WORKSPACE/files-juicefs-upload"
    echo "[$(time_ms)ms] Generating ${SIZE_GB}GB of test files for JuiceFS..."
    cd $GITHUB_WORKSPACE
    npm ci 2>/dev/null || true
    rm -rf "$JUICEFS_FILES_DIR"
    npm run generate -- "$SIZE_GB" "$JUICEFS_FILES_DIR"
    
    JUICEFS_UPLOAD_START=$(time_ms)
    cp -r "$JUICEFS_FILES_DIR"/* "$JUICEFS_MOUNT"/
    JUICEFS_UPLOAD_END=$(time_ms)
    echo "[$(time_ms)ms] JuiceFS upload complete in $((JUICEFS_UPLOAD_END - JUICEFS_UPLOAD_START))ms"
    
    rm -rf "$JUICEFS_FILES_DIR"
fi

# Unmount and remount to clear any local cache for fair test
echo "[$(time_ms)ms] Remounting JuiceFS with fresh cache..."
juicefs umount "$JUICEFS_MOUNT"
rm -rf "$JUICEFS_CACHE_DIR"/*
sleep 1

# Benchmark mount time
echo ""
echo "--- JuiceFS: Benchmark ---"
JUICEFS_MOUNT_START=$(time_ms)

juicefs mount "$JUICEFS_META_URL" "$JUICEFS_MOUNT" \
    --cache-dir "$JUICEFS_CACHE_DIR" \
    --cache-size 4096 \
    --prefetch 3 \
    --buffer-size 300 \
    -d

sleep 1
JUICEFS_MOUNT_END=$(time_ms)
JUICEFS_MOUNT_TIME=$((JUICEFS_MOUNT_END - JUICEFS_MOUNT_START))
echo "[${JUICEFS_MOUNT_TIME}ms] JuiceFS mount complete: ${JUICEFS_MOUNT_TIME}ms"

# Mount overlayfs for writeability
JUICEFS_OVERLAY_START=$(time_ms)
sudo mount -t overlay overlay -o "lowerdir=$JUICEFS_MOUNT,upperdir=$JUICEFS_OVERLAY_UPPER,workdir=$JUICEFS_OVERLAY_WORK" "$JUICEFS_OVERLAY_TARGET"
JUICEFS_OVERLAY_END=$(time_ms)
JUICEFS_OVERLAY_TIME=$((JUICEFS_OVERLAY_END - JUICEFS_OVERLAY_START))
echo "[${JUICEFS_OVERLAY_TIME}ms] overlayfs mount complete: ${JUICEFS_OVERLAY_TIME}ms"

JUICEFS_TOTAL_MOUNT=$((JUICEFS_MOUNT_TIME + JUICEFS_OVERLAY_TIME))
echo "[${JUICEFS_TOTAL_MOUNT}ms] Total mount time (JuiceFS): ${JUICEFS_TOTAL_MOUNT}ms"

# Count files
JUICEFS_TOTAL_FILES=$(find "$JUICEFS_OVERLAY_TARGET" -type f 2>/dev/null | wc -l)
JUICEFS_TOTAL_SIZE_MB=$((JUICEFS_TOTAL_FILES * 2))
echo "Total files: $JUICEFS_TOTAL_FILES ($JUICEFS_TOTAL_SIZE_MB MB)"

# Time to first byte
echo ""
echo "Testing JuiceFS on-demand access..."
JUICEFS_FIRST_START=$(time_ms)
JUICEFS_FIRST_FILE=$(find "$JUICEFS_OVERLAY_TARGET" -type f 2>/dev/null | head -1)
echo "First file: $JUICEFS_FIRST_FILE"
head -c 1 "$JUICEFS_FIRST_FILE" > /dev/null
JUICEFS_FIRST_END=$(time_ms)
JUICEFS_FIRST_TIME=$((JUICEFS_FIRST_END - JUICEFS_FIRST_START))
echo "[${JUICEFS_FIRST_TIME}ms] Time to first byte: ${JUICEFS_FIRST_TIME}ms"

# 10 random files
echo ""
echo "Reading 10 random files..."
JUICEFS_SAMPLE_START=$(time_ms)
SAMPLE_FILES=$(find "$JUICEFS_OVERLAY_TARGET" -type f 2>/dev/null | shuf | head -10)
for f in $SAMPLE_FILES; do
    cat "$f" > /dev/null
done
JUICEFS_SAMPLE_END=$(time_ms)
JUICEFS_SAMPLE_TIME=$((JUICEFS_SAMPLE_END - JUICEFS_SAMPLE_START))
echo "[${JUICEFS_SAMPLE_TIME}ms] 10 random files read in ${JUICEFS_SAMPLE_TIME}ms (avg: $((JUICEFS_SAMPLE_TIME / 10))ms per file)"

# Full hydration (cold)
echo ""
echo "Full hydration (cold cache)..."
JUICEFS_HYDRATE_START=$(time_ms)
FILES_READ=0

FILE_LIST=$(find "$JUICEFS_OVERLAY_TARGET" -type f 2>/dev/null)
for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    if [ $((FILES_READ % 50)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - JUICEFS_HYDRATE_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] JuiceFS read ${FILES_READ}/${JUICEFS_TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

JUICEFS_HYDRATE_END=$(time_ms)
JUICEFS_HYDRATE_TIME=$((JUICEFS_HYDRATE_END - JUICEFS_HYDRATE_START))
JUICEFS_HYDRATE_RATE=$((JUICEFS_TOTAL_SIZE_MB * 1000 / (JUICEFS_HYDRATE_TIME + 1)))
echo "[${JUICEFS_HYDRATE_TIME}ms] Full hydration (JuiceFS cold): ${JUICEFS_HYDRATE_TIME}ms (${JUICEFS_HYDRATE_RATE} MB/s)"

# Full read (warm cache)
echo ""
echo "Full read (warm cache)..."
JUICEFS_WARM_START=$(time_ms)
FILES_READ=0

for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    if [ $((FILES_READ % 100)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - JUICEFS_WARM_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] JuiceFS warm read ${FILES_READ}/${JUICEFS_TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

JUICEFS_WARM_END=$(time_ms)
JUICEFS_WARM_TIME=$((JUICEFS_WARM_END - JUICEFS_WARM_START))
JUICEFS_WARM_RATE=$((JUICEFS_TOTAL_SIZE_MB * 1000 / (JUICEFS_WARM_TIME + 1)))
echo "[${JUICEFS_WARM_TIME}ms] Full read (JuiceFS warm): ${JUICEFS_WARM_TIME}ms (${JUICEFS_WARM_RATE} MB/s)"

# Cleanup
echo ""
echo "[$(time_ms)ms] Cleaning up JuiceFS benchmark..."
sudo umount "$JUICEFS_OVERLAY_TARGET" || true
juicefs umount "$JUICEFS_MOUNT" || true
docker stop redis-juicefs && docker rm redis-juicefs || true
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
echo "INDIVIDUAL FILES (blobfuse2 on-demand):"
echo "  Total mount time:      ${INDIVIDUAL_TOTAL_MOUNT}ms"
echo "  Time to first byte:    ${INDIVIDUAL_FIRST_TIME}ms"
echo "  10 random files:       ${INDIVIDUAL_SAMPLE_TIME}ms (avg: $((INDIVIDUAL_SAMPLE_TIME / 10))ms)"
echo "  Full hydration:        ${INDIVIDUAL_HYDRATE_TIME}ms (${INDIVIDUAL_HYDRATE_RATE} MB/s)"
echo ""
echo "JUICEFS (smart prefetch + block cache):"
echo "  Total mount time:      ${JUICEFS_TOTAL_MOUNT}ms"
echo "  Time to first byte:    ${JUICEFS_FIRST_TIME}ms"
echo "  10 random files:       ${JUICEFS_SAMPLE_TIME}ms (avg: $((JUICEFS_SAMPLE_TIME / 10))ms)"
echo "  Full hydration (cold): ${JUICEFS_HYDRATE_TIME}ms (${JUICEFS_HYDRATE_RATE} MB/s)"
echo "  Full read (warm):      ${JUICEFS_WARM_TIME}ms (${JUICEFS_WARM_RATE} MB/s)"
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
echo "juicefs_mount_time=${JUICEFS_TOTAL_MOUNT}" >> $GITHUB_OUTPUT
echo "juicefs_first_byte=${JUICEFS_FIRST_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_sample_time=${JUICEFS_SAMPLE_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_hydrate_cold=${JUICEFS_HYDRATE_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_hydrate_warm=${JUICEFS_WARM_TIME}" >> $GITHUB_OUTPUT
