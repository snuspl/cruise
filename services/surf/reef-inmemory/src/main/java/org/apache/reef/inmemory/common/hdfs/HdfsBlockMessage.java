package org.apache.reef.inmemory.common.hdfs;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockInfo;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import java.io.Serializable;
import java.util.List;

/**
 * Message sent by driver to Task. Contains a single Block to load into its task
 * and the locations where the Block is replicated in HDFS.
 */
public final class HdfsBlockMessage implements Serializable {

  private final BlockId blockId;
  private final HdfsBlockInfo blockInfo;
  private final List<HdfsDatanodeInfo> locations;
  private final boolean pin;

  public HdfsBlockMessage(final BlockId blockId,
                          final HdfsBlockInfo blockInfo,
                          final List<HdfsDatanodeInfo> locations,
                          final boolean pin) {
    this.blockId = blockId;
    this.blockInfo = blockInfo;
    this.locations = locations;
    this.pin = pin;
  }

  public BlockId getBlockId() {
    return blockId;
  }

  public HdfsBlockInfo getBlockInfo() {
    return blockInfo;
  }

  public List<HdfsDatanodeInfo> getLocations() {
    return locations;
  }

  public boolean isPin() {
    return pin;
  }
}
