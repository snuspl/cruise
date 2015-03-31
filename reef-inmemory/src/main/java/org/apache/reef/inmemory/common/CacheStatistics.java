package org.apache.reef.inmemory.common;

import javax.inject.Inject;
import java.io.Serializable;

/**
 * A set of statistics for the Cache.
 * - cacheBytes: total amount of memory stored in the cache (including pinnedBytes)
 * - copyingBytes: amount of memory currently being copied into the cache
 * - pinnedBytes: amount of memory pinned in the cache
 * NOTE: changes should only be made to statistics through MemoryManager
 */
public final class CacheStatistics implements Serializable {
  private final long maxBytes;
  private long cacheBytes = 0;
  private long copyingBytes = 0;
  private long pinnedBytes = 0;
  private long evictedBytes = 0;

  @Inject
  public CacheStatistics() {
    maxBytes = Runtime.getRuntime().maxMemory();
  }

  public CacheStatistics(final long maxBytes) {
    this.maxBytes = maxBytes;
  }

  public void addCacheBytes(long amount) {
    cacheBytes += amount;
  }

  public void subtractCacheBytes(long amount) {
    cacheBytes -= amount;
  }

  public void resetCacheBytes() {
    cacheBytes = 0;
  }

  public void addCopyingBytes(long amount) {
    copyingBytes += amount;
  }

  public void subtractCopyingBytes(long amount) {
    copyingBytes -= amount;
  }

  public void resetCopyingBytes() {
    copyingBytes = 0;
  }

  public void addPinnedBytes(long amount) {
    pinnedBytes += amount;
  }

  public void subtractPinnedBytes(long amount) {
    pinnedBytes -= amount;
  }

  public void resetPinnedBytes() {
    pinnedBytes = 0;
  }

  public void addEvictedBytes(long amount) {
    evictedBytes += amount;
  }

  public void subtractEvictedBytes(long amount) {
    evictedBytes -= amount;
  }

  public void resetEvictedBytes() {
    evictedBytes = 0;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  public long getCacheBytes() {
    return cacheBytes;
  }

  public long getCopyingBytes() {
    return copyingBytes;
  }

  public long getPinnedBytes() {
    return pinnedBytes;
  }

  public long getEvictedBytes() {
    return evictedBytes;
  }

  @Override
  public String toString() {
    return String.format("{ max: %d, cache : %d, pinned: %d, copying : %d, evicted : %d}",
            maxBytes, cacheBytes, pinnedBytes, copyingBytes, evictedBytes);
  }
}
