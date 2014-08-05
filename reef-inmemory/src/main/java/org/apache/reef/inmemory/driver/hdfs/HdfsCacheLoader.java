package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.CacheMessage;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache Loader implementation for HDFS. The metadata containing HDFS locations
 * is sent to the Tasks. The metadata containing Task locations is then returned
 * to the LoadingCache.
 */
public final class HdfsCacheLoader extends CacheLoader<Path, FileMeta> {

  private static final Logger LOG = Logger.getLogger(HdfsCacheLoader.class.getName());

  private static final ObjectSerializableCodec<CacheMessage> CODEC = new ObjectSerializableCodec<>();

  private final CacheManager cacheManager;
  private final HdfsCacheMessenger cacheMessenger;
  private final HdfsCacheSelectionPolicy cacheSelector;
  private final ReplicationPolicy replicationPolicy;
  private final String dfsAddress;
  private final DFSClient dfsClient;

  @Inject
  public HdfsCacheLoader(final CacheManager cacheManager,
                         final HdfsCacheMessenger cacheMessenger,
                         final HdfsCacheSelectionPolicy cacheSelector,
                         final ReplicationPolicy replicationPolicy,
                         final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.cacheManager = cacheManager;
    this.cacheMessenger = cacheMessenger;
    this.cacheSelector = cacheSelector;
    this.replicationPolicy = replicationPolicy;
    this.dfsAddress = dfsAddress;
    try {
      this.dfsClient = new DFSClient(new URI(this.dfsAddress), new Configuration());
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to DFS Client", ex);
    }
  }

  /*
   * Copies block identifying information into BlockInfo. Does /not/ copy
   * location information (as it is not identifying information).
   */
  private BlockInfo copyBlockInfo(LocatedBlock locatedBlock) throws IOException {
    BlockInfo blockInfo = new BlockInfo();

    blockInfo.setBlockId(locatedBlock.getBlock().getBlockId());
    blockInfo.setOffSet(locatedBlock.getStartOffset());
    blockInfo.setLength(locatedBlock.getBlockSize());
    blockInfo.setNamespaceId(locatedBlock.getBlock().getBlockPoolId());
    blockInfo.setGenerationStamp(locatedBlock.getBlock().getGenerationStamp());
    blockInfo.setToken(locatedBlock.getBlockToken().encodeToUrlString());

    return blockInfo;
  }

  @Override
  public FileMeta load(Path path) throws FileNotFoundException, IOException {
    LOG.log(Level.INFO, "Load in memory: {0}", path);

    final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(path.toString(), 0);

    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFileSize(locatedBlocks.getFileLength());

    for (final LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      final HdfsBlockId hdfsBlock = HdfsBlockId.copyBlock(locatedBlock);
      final List<HdfsDatanodeInfo> hdfsDatanodeInfos =
              HdfsDatanodeInfo.copyDatanodeInfos(locatedBlock.getLocations());
      final HdfsBlockMessage msg = new HdfsBlockMessage(hdfsBlock, hdfsDatanodeInfos);
      final BlockInfo cacheBlock = copyBlockInfo(locatedBlock);

      final List<CacheNode> cacheNodes = cacheManager.getCaches();
      if (cacheNodes.size() == 0) {
        throw new IOException("Surf has zero caches");
      }

      // TODO: add pinning
      final Action action = replicationPolicy.getReplicationAction(path.toString(), fileMeta);
      final int numReplicas;
      if (replicationPolicy.isBroadcast(action)) {
        numReplicas = cacheNodes.size();
      } else {
        numReplicas = action.getFactor();
      }

      final List<CacheNode> selectedNodes = cacheSelector.select(locatedBlock, cacheNodes, numReplicas);
      if (selectedNodes.size() == 0) {
        throw new IOException("Surf selected zero caches out of "+cacheNodes.size()+" total caches");
      }

      for (final CacheNode cacheNode : selectedNodes) {
        cacheMessenger.addBlock(cacheNode.getTaskId(), msg); // TODO: is addBlock a good name?

        final NodeInfo location = new NodeInfo(cacheNode.getAddress(), cacheNode.getRack());
        cacheBlock.addToLocations(location);
      }

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "  " + cacheBlock.toString());
      }
      fileMeta.addToBlocks(cacheBlock);
    }
    return fileMeta;
  }
}
