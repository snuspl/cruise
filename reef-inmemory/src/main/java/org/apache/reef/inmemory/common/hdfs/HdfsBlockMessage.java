package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Message sent by driver to Task. Contains a single Block to load into its task
 * and the locations where the Block is replicated in HDFS.
 */
public final class HdfsBlockMessage implements Serializable {

  private final HdfsBlockId blockId;
  private final List<HdfsDatanodeInfo> locations;
  private final boolean pinned;

  public HdfsBlockMessage(final HdfsBlockId blockId,
                          final List<HdfsDatanodeInfo> locations,
                          final boolean pinned) {
    this.blockId = blockId;
    this.locations = locations;
    this.pinned = pinned;
  }

  public HdfsBlockId getBlockId() {
    return blockId;
  }

  public List<HdfsDatanodeInfo> getLocations() {
    return locations;
  }

  public boolean isPinned() {
    return pinned;
  }
}
