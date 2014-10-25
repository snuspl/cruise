package org.apache.reef.inmemory.driver.write;

import org.apache.reef.inmemory.driver.CacheNode;

import java.util.Collections;
import java.util.List;

/**
 * Choose just one cache node to write data into.
 */
public class WritingRandomCacheSelectionPolicy implements WritingCacheSelectionPolicy {
  @Override
  public CacheNode select(List<CacheNode> nodes) {
    Collections.shuffle(nodes);
    return nodes.get(0);
  }
}
