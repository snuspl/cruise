package org.apache.reef.inmemory.common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A set of statistics for the Cache.
 * - cacheBytes: total amount of memory stored in the cache
 * - loadingBytes: amount of memory currently being loading into the cache
 */
public final class CacheStatistics implements Serializable {

  private AtomicLong cacheBytes = new AtomicLong();
  private AtomicLong loadingBytes = new AtomicLong();

  public void addCacheBytes(long amount) {
    cacheBytes.addAndGet(amount);
  }

  public void subtractCacheBytes(long amount) {
    cacheBytes.addAndGet(-amount);
  }

  public void resetCacheBytes() {
    cacheBytes.set(0);
  }

  public void addLoadingBytes(long amount) {
    loadingBytes.addAndGet(amount);
  }

  public void subtractLoadingBytes(long amount) {
    loadingBytes.addAndGet(-amount);
  }

  public void resetLoadingBytes() {
    loadingBytes.set(0);
  }

  public long getCacheBytes() {
    return cacheBytes.get();
  }

  public long getLoadingBytes() {
    return loadingBytes.get();
  }

  @Override
  public String toString() {
    return String.format("{ cache : %d, loading : %d }",
            cacheBytes.get(), loadingBytes.get());
  }
}
