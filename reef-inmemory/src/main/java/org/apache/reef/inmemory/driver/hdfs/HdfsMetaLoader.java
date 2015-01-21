package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.tang.annotations.Parameter;
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
 * Metadata Loader implementation for HDFS.
 *
 * Invoked by {@link org.apache.reef.inmemory.driver.SurfMetaManager#get}, the CacheLoader
 * gets the file status from HDFS. Metadata is created from the status and then returned
 * to be stored in the LoadingCache.
 */
public final class HdfsMetaLoader extends CacheLoader<Path, FileMeta> implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(HdfsMetaLoader.class.getName());
  private final EventRecorder RECORD;

  private final CacheManager cacheManager;
  private final HdfsCacheMessenger cacheMessenger;
  private final HdfsCacheSelectionPolicy cacheSelector;
  private final HdfsBlockIdFactory blockFactory;
  private final ReplicationPolicy replicationPolicy;
  private final String dfsAddress;
  private final DFSClient dfsClient;

  @Inject
  public HdfsMetaLoader(final CacheManager cacheManager,
                        final HdfsCacheMessenger cacheMessenger,
                        final HdfsCacheSelectionPolicy cacheSelector,
                        final HdfsBlockIdFactory blockFactory,
                        final ReplicationPolicy replicationPolicy,
                        final @Parameter(DfsParameters.Address.class) String dfsAddress,
                        final EventRecorder recorder) {
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

    this.RECORD = recorder;
  }

  @Override
  public FileMeta load(final Path path) throws FileNotFoundException, IOException {
    final String pathStr = path.toString();
    LOG.log(Level.INFO, "Load in memory: {0}", pathStr);

    final Event getFileInfoEvent = RECORD.event("driver.get-file-info", pathStr).start();
    // getFileInfo returns null if FileNotFound, as stated in its javadoc
    final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(pathStr);
    if (hdfsFileStatus == null) {
      throw new FileNotFoundException(pathStr);
    }
    final long len = hdfsFileStatus.getLen();
    final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(pathStr, 0, len);
    RECORD.record(getFileInfoEvent.stop());

    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFileSize(locatedBlocks.getFileLength());
    fileMeta.setBlockSize(hdfsFileStatus.getBlockSize());
    fileMeta.setFullPath(pathStr);

    final List<CacheNode> cacheNodes = cacheManager.getCaches();
    if (cacheNodes.size() == 0) {
      throw new IOException("Surf has zero caches");
    }

    final Event resolveReplicationPolicyEvent = RECORD.event("driver.resolve-replication-policy", pathStr).start();
    // Resolve replication policy
    final Action action = replicationPolicy.getReplicationAction(pathStr, fileMeta);
    final boolean pin = action.getPin();
    final int numReplicas;
    if (replicationPolicy.isBroadcast(action)) {
      numReplicas = cacheNodes.size();
    } else {
      numReplicas = action.getCacheReplicationFactor();
    }
    RECORD.record(resolveReplicationPolicyEvent.stop());

    final Event addBlocksToCachesEvent = RECORD.event("driver.add-blocks-to-caches", pathStr).start();
    final List<LocatedBlock> locatedBlockList = locatedBlocks.getLocatedBlocks();

    final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(locatedBlockList, cacheNodes, numReplicas);
    for (final LocatedBlock locatedBlock : locatedBlockList) {
      final List<CacheNode> selectedNodes = selected.get(locatedBlock);

      if (selectedNodes.size() == 0) {
        throw new IOException("Surf selected zero caches out of "+cacheNodes.size()+" total caches");
      }

      final BlockInfo blockInfo = blockFactory.newBlockInfo(pathStr, locatedBlock);
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
    RECORD.record(addBlocksToCachesEvent.stop());
    return fileMeta;
  }

  @Override
  public void close() throws Exception {
    dfsClient.close();
  }
}
