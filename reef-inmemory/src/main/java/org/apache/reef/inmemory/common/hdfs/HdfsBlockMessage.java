package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.inmemory.task.hdfs.HdfsBlockInfo;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Message sent by driver to Task. Contains a single Block to load into its task
 * and the locations where the Block is replicated in HDFS.
 */
public final class HdfsBlockMessage implements Serializable {

  private final HdfsBlockInfo blockId;
  private final List<HdfsDatanodeInfo> locations;
  private final boolean pin;

  public HdfsBlockMessage(final HdfsBlockInfo blockId,
                          final List<HdfsDatanodeInfo> locations,
                          final boolean pin) {
    this.blockId = blockId;
    this.locations = locations;
    this.pin = pin;
  }

  public HdfsBlockInfo getBlockId() {
    return blockId;
  }

  public List<HdfsDatanodeInfo> getLocations() {
    return locations;
  }

  public boolean isPin() {
    return pin;
  }
}
