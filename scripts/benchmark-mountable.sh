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
# Benchmark 1: Mount with NO local caching
#############################################
echo "=== Benchmark 1: Streaming (no local cache) ==="

# Debug output
echo "DEBUG: Account=$ACCOUNT"
echo "DEBUG: Container=$CONTAINER"
echo "DEBUG: Blob=$BLOB_NAME"
echo "DEBUG: SAS token length=${#SAS_TOKEN}"

MOUNT_START=$(time_ms)

# Create blobfuse2 config for streaming (no cache)
# Note: SAS token must be quoted to handle special characters
cat > /tmp/blobfuse-streaming.yaml << EOF
allow-other: true
logging:
  type: syslog
  level: log_debug
components:
  - libfuse
  - azstorage
libfuse:
  attribute-expiration-sec: 0
  entry-expiration-sec: 0
  negative-entry-expiration-sec: 0
azstorage:
  type: block
  account-name: ${ACCOUNT}
  container: ${CONTAINER}
  endpoint: https://${ACCOUNT}.blob.core.windows.net
  mode: sas
  sas: "${SAS_TOKEN}"
EOF

echo "DEBUG: blobfuse2 config:"
cat /tmp/blobfuse-streaming.yaml

sudo blobfuse2 mount "$MOUNT_POINT" --config-file=/tmp/blobfuse-streaming.yaml --read-only

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
echo "[${OVERLAY_END}ms] Total mount time (streaming): ${TOTAL_MOUNT_TIME}ms"

# Time to first file
FIRST_FILE_START=$(time_ms)
FIRST_FILE=$(find "$OVERLAY_TARGET" -type f 2>/dev/null | head -1)
echo "First file: $FIRST_FILE"
head -c 1 "$FIRST_FILE" > /dev/null
FIRST_FILE_END=$(time_ms)
FIRST_FILE_TIME=$((FIRST_FILE_END - FIRST_FILE_START))
echo "[${FIRST_FILE_END}ms] Time to first byte: ${FIRST_FILE_TIME}ms"

# Count total files for progress
TOTAL_FILES=$(find "$OVERLAY_TARGET" -type f 2>/dev/null | wc -l)
TOTAL_SIZE_MB=$((TOTAL_FILES * 2)) # 2MB per file
echo "Total files to hydrate: $TOTAL_FILES ($TOTAL_SIZE_MB MB)"

# Full read (hydration) - simple sequential with logging
echo "Starting full hydration (streaming)..."
HYDRATE_START=$(time_ms)
FILES_READ=0

echo "[0ms] Beginning file enumeration..."
FILE_LIST=$(find "$OVERLAY_TARGET" -type f 2>/dev/null)
ENUM_TIME=$(($(time_ms) - HYDRATE_START))
echo "[${ENUM_TIME}ms] File enumeration complete"

echo "[${ENUM_TIME}ms] Beginning sequential reads..."
for file in $FILE_LIST; do
    cat "$file" > /dev/null
    FILES_READ=$((FILES_READ + 1))
    
    # Log every 10 files
    if [ $((FILES_READ % 10)) -eq 0 ]; then
        ELAPSED=$(($(time_ms) - HYDRATE_START))
        MB_READ=$((FILES_READ * 2))
        RATE=$((MB_READ * 1000 / (ELAPSED + 1)))
        echo "[${ELAPSED}ms] Read ${FILES_READ}/${TOTAL_FILES} files, ${MB_READ}MB (${RATE} MB/s)"
    fi
done

HYDRATE_END=$(time_ms)
HYDRATE_TIME=$((HYDRATE_END - HYDRATE_START))
HYDRATE_RATE=$((TOTAL_SIZE_MB * 1000 / (HYDRATE_TIME + 1)))
echo "[${HYDRATE_TIME}ms] Full hydration (streaming) complete: ${HYDRATE_TIME}ms (${HYDRATE_RATE} MB/s)"

echo ""
echo "[$(time_ms)ms] Starting cleanup for streaming benchmark..."

# Cleanup
echo "[$(time_ms)ms] Unmounting overlay..."
sudo umount "$OVERLAY_TARGET" || true
echo "[$(time_ms)ms] Unmounting squashfs..."
sudo umount "$SQUASHFS_MOUNT" || true
echo "[$(time_ms)ms] Unmounting blobfuse2..."
sudo blobfuse2 unmount "$MOUNT_POINT" || true
echo "[$(time_ms)ms] Cleaning up temp dirs..."
sudo rm -rf "$OVERLAY_UPPER"/* "$OVERLAY_WORK"/*
echo "[$(time_ms)ms] Cleanup complete"

STREAMING_MOUNT_TIME=$TOTAL_MOUNT_TIME
STREAMING_FIRST_BYTE=$FIRST_FILE_TIME
STREAMING_HYDRATE=$HYDRATE_TIME

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
# Output results
#############################################
echo ""
echo "=========================================="
echo "        MOUNTABLE CACHE BENCHMARK"
echo "=========================================="
echo ""
echo "STREAMING MODE (no local cache):"
echo "  Total mount time:    ${STREAMING_MOUNT_TIME}ms"
echo "  Time to first byte:  ${STREAMING_FIRST_BYTE}ms"
echo "  Full hydration:      ${STREAMING_HYDRATE}ms"
echo ""
echo "CACHED MODE (blobfuse2 file cache):"
echo "  Total mount time:    ${CACHED_MOUNT_TIME}ms"
echo "  Time to first byte:  ${CACHED_FIRST_BYTE}ms"
echo "  Full hydration (cold): ${CACHED_HYDRATE_COLD}ms"
echo "  Full read (warm):    ${CACHED_HYDRATE_WARM}ms"
echo ""
echo "=========================================="

# Output as GitHub Actions step outputs
echo "streaming_mount_time=${STREAMING_MOUNT_TIME}" >> $GITHUB_OUTPUT
echo "streaming_first_byte=${STREAMING_FIRST_BYTE}" >> $GITHUB_OUTPUT
echo "streaming_hydrate=${STREAMING_HYDRATE}" >> $GITHUB_OUTPUT
echo "cached_mount_time=${CACHED_MOUNT_TIME}" >> $GITHUB_OUTPUT
echo "cached_first_byte=${CACHED_FIRST_BYTE}" >> $GITHUB_OUTPUT
echo "cached_hydrate_cold=${CACHED_HYDRATE_COLD}" >> $GITHUB_OUTPUT
echo "cached_hydrate_warm=${CACHED_HYDRATE_WARM}" >> $GITHUB_OUTPUT
