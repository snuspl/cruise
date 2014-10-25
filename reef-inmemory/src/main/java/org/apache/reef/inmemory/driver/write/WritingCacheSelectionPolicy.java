package org.apache.reef.inmemory.driver.write;

import org.apache.reef.inmemory.driver.CacheNode;

import java.util.List;

/**
 * Choose one of the cache nodes to write a block
 */
public interface WritingCacheSelectionPolicy {
  // TODO Do we need to consider the locality? Does it seem possible?
  public CacheNode select(final List<CacheNode> nodes);
}
