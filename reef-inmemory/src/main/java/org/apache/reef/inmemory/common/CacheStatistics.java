package org.apache.reef.inmemory.common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class CacheStatistics implements Serializable {

  private AtomicInteger cacheMB = new AtomicInteger();
  private AtomicInteger loadingMB = new AtomicInteger();

  public void addCacheMB(int add) {
    cacheMB.addAndGet(add);
  }

  public void subtractCacheMB(int sub) {
    cacheMB.addAndGet(-sub);
  }

  public void resetCacheMB() {
    cacheMB.set(0);
  }

  public void addLoadingMB(int add) {
    loadingMB.addAndGet(add);
  }

  public synchronized void subtractLoadingMB(int sub) {
    loadingMB.addAndGet(-sub);
  }

  public void resetLoadingMB() {
    loadingMB.set(0);
  }

  public int getCacheMB() {
    return cacheMB.get();
  }

  public int getLoadingMB() {
    return loadingMB.get();
  }

  @Override
  public String toString() {
    return String.format("{ cache : %d, loading : %d }",
            cacheMB.get(), loadingMB.get());
  }
}
