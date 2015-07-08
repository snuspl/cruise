package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import java.util.List;
import java.util.Map;

/**
 * Interface for sending messages to Cache nodes.
 * A new implementation should be create for each Base FS, because
 * the block information <T> is FS-dependant.
 */
public interface CacheNodeMessenger<T> {
  /**
   * Remove blocks stored at the Cache
   */
  void clear(String taskId);

  /**
   * Remove blocks stored at all Caches
   */
  void clearAll();

  /**
   * Send a block load request to the Cache
   */
  void addBlock(String taskId, T msg);

  /**
   * Send requests of removing blocks to the Caches
   */
  void deleteBlocks(Map<NodeInfo, List<BlockId>> nodeToBlocks);
}
