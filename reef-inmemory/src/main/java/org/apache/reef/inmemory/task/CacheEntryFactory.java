package org.apache.reef.inmemory.task;

import javax.inject.Inject;

/**
 * Create a cache entry to load/write data.
 */
public class CacheEntryFactory {

  @Inject
  public CacheEntryFactory() {
  }

  /**
   * Create a cache entry that loads data from BaseFS.
   */
  public static CacheEntry createEntry(BlockLoader blockLoader) {
    return new LoadableCacheEntry(blockLoader);
  }

  /**
   * Create a cache entry that writes data from Client.
   */
  public static CacheEntry createEntry(BlockWriter blockWriter) {
    return new WritableCacheEntry(blockWriter);
  }
}
