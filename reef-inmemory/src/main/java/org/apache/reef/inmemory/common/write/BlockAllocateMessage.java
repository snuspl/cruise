package org.apache.reef.inmemory.common.write;

import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.task.BlockId;

import java.io.Serializable;

/**
 * Message sent by driver to Task to allocate a block.
 * TODO Is Action a Serializable class?
 */
public class BlockAllocateMessage implements Serializable {
  private BlockId blockId;
  private Action action;

  public BlockAllocateMessage(final BlockId blockId, final Action action) {
    this.blockId = blockId;
    this.action = action;
  }

  public BlockId getBlockId() {
    return blockId;
  }

  public Action getAction() {
    return action;
  }
}
