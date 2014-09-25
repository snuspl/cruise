package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache Loader implementation for HDFS.
 *
 * Tasks load data from HDFS based on the HDFS metadata sent here.
 * The metadata, including Task locations, is then returned
 * to be stored in the LoadingCache.
 */
public final class HdfsCacheLoader extends CacheLoader<Path, FileMeta> implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(HdfsCacheLoader.class.getName());

  private final CacheManager cacheManager;
  private final HdfsCacheMessenger cacheMessenger;
  private final HdfsCacheSelectionPolicy cacheSelector;
  private final HdfsBlockIdFactory blockFactory;
  private final ReplicationPolicy replicationPolicy;
  private final String dfsAddress;
  private final DFSClient dfsClient;

  @Inject
  public HdfsCacheLoader(final CacheManager cacheManager,
                         final HdfsCacheMessenger cacheMessenger,
                         final HdfsCacheSelectionPolicy cacheSelector,
                         final HdfsBlockIdFactory blockFactory,
                         final ReplicationPolicy replicationPolicy,
                         final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.cacheManager = cacheManager;
    this.cacheMessenger = cacheMessenger;
    this.cacheSelector = cacheSelector;
    this.blockFactory = blockFactory;
    this.replicationPolicy = replicationPolicy;
    this.dfsAddress = dfsAddress;
    try {
      this.dfsClient = new DFSClient(new URI(this.dfsAddress), new Configuration());
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to DFS Client", ex);
    }
  }

  @Override
  public FileMeta load(Path path) throws FileNotFoundException, IOException {
    LOG.log(Level.INFO, "Load in memory: {0}", path);
    // getFileInfo returns null if FileNotFound, as stated in its javadoc
    final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(path.toString());
    if (hdfsFileStatus == null) {
      throw new FileNotFoundException(path.toString());
    }
    final long len = hdfsFileStatus.getLen();
    final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(path.toString(), 0, len);

    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFileSize(locatedBlocks.getFileLength());
    fileMeta.setBlockSize(hdfsFileStatus.getBlockSize());
    fileMeta.setFullPath(path.toString());

    final List<CacheNode> cacheNodes = cacheManager.getCaches();
    if (cacheNodes.size() == 0) {
      throw new IOException("Surf has zero caches");
    }

    // Resolve replication policy
    final Action action = replicationPolicy.getReplicationAction(path.toString(), fileMeta);
    final boolean pin = action.getPin();
    final int numReplicas;
    if (replicationPolicy.isBroadcast(action)) {
      numReplicas = cacheNodes.size();
    } else {
      numReplicas = action.getCacheReplicationFactor();
    }

    final List<LocatedBlock> locatedBlockList = locatedBlocks.getLocatedBlocks();

    final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(locatedBlockList, cacheNodes, numReplicas);
    for (final LocatedBlock locatedBlock : locatedBlockList) {
      final List<CacheNode> selectedNodes = selected.get(locatedBlock);

      if (selectedNodes.size() == 0) {
        throw new IOException("Surf selected zero caches out of "+cacheNodes.size()+" total caches");
      }

      final BlockInfo blockInfo = blockFactory.newBlockInfo(path.toString(), locatedBlock);
      final HdfsBlockId hdfsBlockId = blockFactory.newBlockId(blockInfo);
      final List<HdfsDatanodeInfo> hdfsDatanodeInfos =
              HdfsDatanodeInfo.copyDatanodeInfos(locatedBlock.getLocations());
      final HdfsBlockMessage msg = new HdfsBlockMessage(hdfsBlockId, hdfsDatanodeInfos, pin);

      for (final CacheNode cacheNode : selectedNodes) {
        cacheMessenger.addBlock(cacheNode.getTaskId(), msg);

        final NodeInfo location = new NodeInfo(cacheNode.getAddress(), cacheNode.getRack());
        blockInfo.addToLocations(location);
      }

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "  " + blockInfo.toString());
      }
      fileMeta.addToBlocks(blockInfo);
    }
    return fileMeta;
  }

  @Override
  public void close() throws Exception {
    dfsClient.close();
  }
}
