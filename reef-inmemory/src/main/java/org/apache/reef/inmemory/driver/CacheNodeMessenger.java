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
  public void clear(String taskId);

  /**
   * Remove blocks stored at all Caches
   */
  public void clearAll();

  /**
   * Send a block load request to the Cache
   */
  public void addBlock(String taskId, T msg);

  /**
   * Remove blocks in the Caches
   */
  public void deleteBlocks(Map<NodeInfo, List<BlockId>> blockIds); // TODO Better parameter name
}
