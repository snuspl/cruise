package org.apache.reef.inmemory.cache.hdfs;

import org.apache.reef.inmemory.cache.BlockId;

import java.io.Serializable;
import java.util.List;

/**
 * Message sent by Driver to Task. Contains a single Block to load into its cache
 * and the locations where the Block is replicated in HDFS.
 */
public final class HdfsBlockMessage implements Serializable {

  private final HdfsBlockId blockId;
  private final List<HdfsDatanodeInfo> locations;

  public HdfsBlockMessage(final HdfsBlockId blockId,
                          final List<HdfsDatanodeInfo> locations) {
    this.blockId = blockId;
    this.locations = locations;
  }

  public BlockId getBlockId() {
    return blockId;
  }

  public List<HdfsDatanodeInfo> getLocations() {
    return locations;
  }
}
