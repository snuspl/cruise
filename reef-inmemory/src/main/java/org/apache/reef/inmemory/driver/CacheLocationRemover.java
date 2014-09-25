package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.task.BlockId;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CacheLocationRemover {

  @Inject
  public CacheLocationRemover() {
  }

  private final Map<String, Map<BlockId, List<String>>> pendingRemoves = new HashMap<>();

  // TODO: synchronized needed?
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

  public synchronized Map<BlockId, List<String>> pullPendingRemoves(final String filePath) {
    final Map<BlockId, List<String>> toReturn = pendingRemoves.get(filePath);
    pendingRemoves.remove(filePath);
    return toReturn;
  }
}
