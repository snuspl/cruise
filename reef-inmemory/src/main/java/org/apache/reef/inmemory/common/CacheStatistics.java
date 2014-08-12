package org.apache.reef.inmemory.common;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A set of statistics for the Cache.
 * - cacheBytes: total amount of memory stored in the cache (including pinnedBytes)
 * - loadingBytes: amount of memory currently being loading into the cache
 * - pinnedBytes: amount of memory pinned in the cache
 * NOTE: changes should only be made to statistics through MemoryManager
 */
public final class CacheStatistics implements Serializable {
  private long cacheBytes = 0;
  private long loadingBytes = 0;
  private long pinnedBytes = 0;
  private long evictedBytes = 0;

  @Inject
  public CacheStatistics() {
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

  public void addLoadingBytes(long amount) {
    loadingBytes += amount;
  }

  public void subtractLoadingBytes(long amount) {
    loadingBytes -= amount;
  }

  public void resetLoadingBytes() {
    loadingBytes = 0;
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

  public long getCacheBytes() {
    return cacheBytes;
  }

  public long getLoadingBytes() {
    return loadingBytes;
  }

  public long getPinnedBytes() {
    return pinnedBytes;
  }

  public long getEvictedBytes() {
    return evictedBytes;
  }

  @Override
  public String toString() {
    return String.format("{ cache : %d, pinned: %d, loading : %d, evicted : %d}",
            cacheBytes, pinnedBytes, loadingBytes, evictedBytes);
  }
}
