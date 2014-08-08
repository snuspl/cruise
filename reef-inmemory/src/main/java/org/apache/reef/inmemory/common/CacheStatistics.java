package org.apache.reef.inmemory.common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A set of statistics for the Cache.
 * - cacheMB: total amount of memory stored in the cache
 * - loadingMB: amount of memory currently being loading into the cache
 */
public final class CacheStatistics implements Serializable {

  private AtomicLong cacheMB = new AtomicLong();
  private AtomicLong loadingMB = new AtomicLong();

  public void addCacheMB(long amount) {
    cacheMB.addAndGet(amount);
  }

  public void subtractCacheMB(long amount) {
    cacheMB.addAndGet(-amount);
  }

  public void resetCacheMB() {
    cacheMB.set(0);
  }

  public void addLoadingMB(long amount) {
    loadingMB.addAndGet(amount);
  }

  public void subtractLoadingMB(long amount) {
    loadingMB.addAndGet(-amount);
  }

  public void resetLoadingMB() {
    loadingMB.set(0);
  }

  public long getCacheMB() {
    return cacheMB.get();
  }

  public long getLoadingMB() {
    return loadingMB.get();
  }

  @Override
  public String toString() {
    return String.format("{ cache : %d, loading : %d }",
            cacheMB.get(), loadingMB.get());
  }
}
