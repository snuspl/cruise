package org.apache.reef.inmemory.driver;


import org.apache.reef.inmemory.common.BlockId;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to remove metadata of locations where blocks are no longer stored.
 * The Driver receives notices from Tasks when blocks are removed (e.g. for eviction).
 * This implementation does not edit metadata directly.
 * Instead it logs removals; a caller will "pull" the logs to observe each item exactly once.
 */
public final class CacheLocationRemover {

  @Inject
  public CacheLocationRemover() {
  }

  private final Map<String, Map<BlockId, List<String>>> pendingRemoves = new HashMap<>();

  /**
   * Remove metadata of location where block is no longer stored.
   * Does not edit metadata directly, but rather logs the removal information.
   * @param filePath File's path
   * @param blockId Block's ID
   * @param nodeAddress Location's address
   */
  public synchronized void remove(final String filePath, final BlockId blockId, final String nodeAddress) {
    // Get blockMap (create if needed)
    Map<BlockId, List<String>> blockMap = pendingRemoves.get(filePath);
    if (blockMap == null) {
      blockMap = new HashMap<>();
      pendingRemoves.put(filePath, blockMap);
    }

    // Get locations (create if needed)
    List<String> locations = blockMap.get(blockId);
    if (locations == null) {
      locations = new ArrayList<>();
      blockMap.put(blockId, locations);
    }

    // Add location
    locations.add(nodeAddress);
  }

  /**
   * Pull all pending removals for the given file path.
   * The pending removals returned are removed.
   * @param filePath File path to receive logs on.
   * @return Per each BlockId, the list of locations removed.
   */
  public synchronized Map<BlockId, List<String>> pullPendingRemoves(final String filePath) {
    final Map<BlockId, List<String>> toReturn = pendingRemoves.get(filePath);
    pendingRemoves.remove(filePath);
    return toReturn;
  }
}
