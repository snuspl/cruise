package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheLoader;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache Loader implementation for HDFS. The metadata containing HDFS locations
 * is sent to the Tasks. The metadata containing Task locations is then returned
 * to the LoadingCache.
 */
public final class HdfsCacheLoader extends CacheLoader<Path, FileMeta> {

  private static final Logger LOG = Logger.getLogger(HdfsCacheLoader.class.getName());

  private final HdfsCacheManager cacheManager;
  private final int cachePort;
  private final String dfsAddress;
  private final DFSClient dfsClient;

  @Inject
  public HdfsCacheLoader(final HdfsCacheManager cacheManager,
                         final @Parameter(CacheParameters.Port.class) int cachePort,
                         final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.cacheManager = cacheManager;
    this.cachePort = cachePort;
    this.dfsAddress = dfsAddress;
    try {
      this.dfsClient = new DFSClient(new URI(this.dfsAddress), new Configuration());
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to DFS Client", ex);
    }
  }

  private List<String> getLocations(LocatedBlock locatedBlock) {
    List<String> locations = new ArrayList<>(locatedBlock.getLocations().length);
    for (DatanodeInfo dnInfo : locatedBlock.getLocations()) {
      locations.add(dnInfo.getNetworkLocation());
    }
    return locations;
  }

  /*
   * Copies block identifying information into BlockInfo. Does /not/ copy
   * location information (as it is not identifying information).
   */
  private BlockInfo copyBlockInfo(LocatedBlock locatedBlock) {
    BlockInfo blockInfo = new BlockInfo();

    blockInfo.setBlockId(locatedBlock.getBlock().getBlockId());
    blockInfo.setOffSet(locatedBlock.getStartOffset());
    blockInfo.setLength(locatedBlock.getBlockSize()); // TODO: make length long?
    blockInfo.setNamespaceId(locatedBlock.getBlock().getBlockPoolId());
    blockInfo.setGenerationStamp(locatedBlock.getBlock().getGenerationStamp());

    return blockInfo;
  }

  @Override
  public FileMeta load(Path path) throws FileNotFoundException, IOException {
    LOG.log(Level.INFO, "Load in memory: {0}", path);

    FileMeta fileMeta = new FileMeta();

    LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(path.toString(), 0);
    for (final LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      final BlockInfo hdfsBlock = copyBlockInfo(locatedBlock);
      hdfsBlock.setLocations(getLocations(locatedBlock)); // Add HDFS location info

      final BlockInfo cacheBlock = copyBlockInfo(locatedBlock);
      for (final RunningTask task : cacheManager.getTasksToCache(locatedBlock)) {
        cacheManager.sendToTask(task, hdfsBlock);
        cacheBlock.addToLocations( // Add Cache node location info
                cacheManager.getCacheHost(task) + ":" + cachePort);
      }

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "  " + cacheBlock.toString());
      }
      fileMeta.addToBlocks(cacheBlock);
    }
    return fileMeta;
  }
}
