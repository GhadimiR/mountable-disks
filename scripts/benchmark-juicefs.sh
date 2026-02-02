#!/bin/bash
# JuiceFS benchmark - runs on GitHub Actions runner
set -e

BLOB_SAS_URL="$1"
SIZE_GB="${2:-2}"
AZURE_STORAGE_KEY="$3"  # Optional: Azure Storage Account Key for JuiceFS

# Timing helper
time_ms() {
    echo $(($(date +%s%3N)))
}

echo "=== JuiceFS Benchmark ==="
echo ""

# Parse container SAS URL
CONTAINER_URL_NO_SAS="${BLOB_SAS_URL%%\?*}"
SAS_TOKEN="${BLOB_SAS_URL#*\?}"
ACCOUNT=$(echo "$CONTAINER_URL_NO_SAS" | sed -E 's|https://([^.]+)\.blob\.core\.windows\.net/.*|\1|')
CONTAINER=$(echo "$CONTAINER_URL_NO_SAS" | sed -E 's|https://[^/]+/||' | sed 's|/$||')

echo "Parsed URL:"
echo "  Account: $ACCOUNT"
echo "  Container: $CONTAINER"
echo "  Size: ${SIZE_GB}GB"
echo ""

JUICEFS_MOUNT="/tmp/juicefs-mount"
JUICEFS_OVERLAY_TARGET="./files-juicefs"
JUICEFS_OVERLAY_UPPER="/tmp/juicefs-overlay-upper"
JUICEFS_OVERLAY_WORK="/tmp/juicefs-overlay-work"
JUICEFS_CACHE_DIR="/tmp/juicefs-cache"

# Use /tmp instead of /mnt to avoid permission issues
mkdir -p "$JUICEFS_MOUNT" "$JUICEFS_OVERLAY_TARGET" "$JUICEFS_OVERLAY_UPPER" "$JUICEFS_OVERLAY_WORK" "$JUICEFS_CACHE_DIR"

# Use unique volume name with timestamp to avoid "storage not empty" errors
TIMESTAMP=$(date +%Y%m%d%H%M%S)
JUICEFS_NAME="bench-${SIZE_GB}gb-${TIMESTAMP}"

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

# Always destroy and recreate to avoid stale data issues
if juicefs status "$JUICEFS_META_URL" 2>/dev/null; then
    echo "[$(time_ms)ms] Destroying existing JuiceFS filesystem..."
    juicefs destroy "$JUICEFS_META_URL" "$JUICEFS_NAME" --force 2>/dev/null || true
    sleep 1
fi

echo "[$(time_ms)ms] Formatting JuiceFS filesystem..."

if [ -n "$AZURE_STORAGE_KEY" ]; then
    # Use Storage Account Key (recommended)
    echo "Using Azure Storage Account Key for authentication"
    juicefs format \
        --storage wasb \
        --bucket "https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}" \
        --access-key "${ACCOUNT}" \
        --secret-key "${AZURE_STORAGE_KEY}" \
        --block-size 4096 \
        --compress none \
        --force \
        "$JUICEFS_META_URL" \
        "$JUICEFS_NAME"
else
    # Fallback: try SAS token in URL (may not work)
    echo "WARNING: No storage key provided, trying SAS token in URL (may not work)"
    juicefs format \
        --storage wasb \
        --bucket "https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}?${SAS_TOKEN}" \
        --block-size 4096 \
        --compress none \
        --force \
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
echo "        JUICEFS BENCHMARK"
echo "=========================================="
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
echo "juicefs_mount_time=${JUICEFS_TOTAL_MOUNT}" >> $GITHUB_OUTPUT
echo "juicefs_first_byte=${JUICEFS_FIRST_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_sample_time=${JUICEFS_SAMPLE_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_hydrate_cold=${JUICEFS_HYDRATE_TIME}" >> $GITHUB_OUTPUT
echo "juicefs_hydrate_warm=${JUICEFS_WARM_TIME}" >> $GITHUB_OUTPUT
