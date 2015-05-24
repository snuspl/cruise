package org.apache.reef.inmemory.common;

import java.io.Serializable;
import java.util.List;

/**
 * Message sent from driver to Task to delete Blocks.
 */
public class BlocksDeleteMessage implements Serializable {
  private final List<BlockId> blockIds;

  /**
   * Create a message to delete blocks in a cache node.
   * @param blockIds Block ids that are loaded in the cache node.
   */
  public BlocksDeleteMessage(final List<BlockId> blockIds) {
    this.blockIds = blockIds;
  }

  /**
   * @return The list of block ids to delete.
   */
  public List<BlockId> getBlockIds() {
    return blockIds;
  }
}
