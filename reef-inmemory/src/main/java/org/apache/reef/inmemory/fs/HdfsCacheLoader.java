package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheLoader;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.Launch;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache Loader implementation for HDFS. The metadata containing HDFS locations
 * is sent to the Tasks. The metadata containing Task locations is then returned
 * to the LoadingCache.
 */
public final class HdfsCacheLoader extends CacheLoader<Path, FileMeta> {

  private static final Logger LOG = Logger.getLogger(HdfsCacheLoader.class.getName());

  // TODO: Need object that talks to Task
  private final DFSClient client;

  @Inject
  public HdfsCacheLoader(final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    try {
      this.client = new DFSClient(new URI(dfsAddress), new Configuration());
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to DFS Client", ex);
    }
  }

  @Override
  public FileMeta load(Path path) throws FileNotFoundException, IOException {
    LOG.log(Level.INFO, "Load in memory: {0}", path);
    LocatedBlocks locatedBlocks = client.getLocatedBlocks(path.toString(), 0);

    FileMeta fileMeta = new FileMeta();
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      BlockInfo blockInfo = getBlockInfo(locatedBlock);
      fileMeta.addToBlocks(blockInfo);
    }

    // TODO: Send this information to Task, and wait for Task to load the block

    return fileMeta;
  }

  private BlockInfo getBlockInfo(LocatedBlock locatedBlock) {
    BlockInfo blockInfo = new BlockInfo();
    blockInfo.setBlockId(locatedBlock.getBlock().getBlockId());
    blockInfo.setLength((int)locatedBlock.getBlockSize()); // TODO: make length long?
    for (DatanodeInfo locationInfo : locatedBlock.getLocations()) {
      blockInfo.addToLocations(locationInfo.getNetworkLocation());
    }
    return blockInfo;
  }
}
