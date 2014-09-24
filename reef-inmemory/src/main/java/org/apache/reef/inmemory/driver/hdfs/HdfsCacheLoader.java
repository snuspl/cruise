package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cache Loader implementation for HDFS. The metadata containing HDFS locations
 * is sent to the Tasks. The metadata containing Task locations is then returned
 * to the LoadingCache.
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

  // TODO: This HashSet may be slow because FileMeta hashCode is defined as 0
  private final Set<FileMeta> asyncReloadInProgress = new HashSet<>();
  // TODO: make configurable
  private final ExecutorService executor = Executors.newFixedThreadPool(3);

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
      numReplicas = action.getFactor();
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
        cacheMessenger.addBlock(cacheNode.getTaskId(), msg); // TODO: is addBlock a good name?

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

  /**
   * 1. Resolve replication policy
   * 2. Find blocks that have not fulfilled their replication factor, i.e. need loading
   * -  Get information from HDFS
   * 3. For each block that needs loading, synchronously add one location
   * 4. Return the new metadata. If there are still blocks that need replicas,
   *    then asynchronously add those locations.
   *
   * Steps 1-3 are intentionally synchronous to avoid replying with blocks with zero locations.
   * Step 4 is asynchronous, concurrent updates to the same metadata is avoided by using locking
   *        (implemented as synchronized blocks on entry and exit).
   *
   * @param path
   * @param fileMeta
   * @return
   * @throws Exception
   */
  @Override
  public ListenableFuture<FileMeta> reload(final Path path, final FileMeta fileMeta) throws Exception {
    if (fileMeta.getBlocksSize() == 0) { // TODO: this could cause null pointer, when run concurrently with removeLocations!
      return Futures.immediateFuture(fileMeta);
    }

    final List<CacheNode> cacheNodes = cacheManager.getCaches();
    if (cacheNodes.size() == 0) {
      throw new IOException("Surf has zero caches");
    }

    // Resolve replication policy
    final Action action = replicationPolicy.getReplicationAction(path.toString(), fileMeta);
    final boolean pin = action.getPin();
    final int replicationFactor;
    if (replicationPolicy.isBroadcast(action)) {
      replicationFactor = cacheNodes.size();
    } else {
      replicationFactor = action.getFactor();
    }

    // Find blocks that have not fulfilled their replication factor, i.e. need loading
    final List<Integer> loadingNeeded = new ArrayList<>();
    final List<BlockInfo> blockInfos = fileMeta.getBlocks();
    for (int i = 0; i < blockInfos.size(); i++) {
      final BlockInfo blockInfo = blockInfos.get(i); // TODO: this could cause index out of bounds, when run concurrently with removeLocations!
      if (blockInfo.getLocationsSize() < replicationFactor) { // TODO: this could cause null pointer, when run concurrently with removeLocations!
        loadingNeeded.add(i);
      }
    }

    if (loadingNeeded.size() == 0) {
      return Futures.immediateFuture(fileMeta);
    }

    // Get information from HDFS
    final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(path.toString());
    if (hdfsFileStatus == null) {
      throw new FileNotFoundException(path.toString());
    }
    final long len = hdfsFileStatus.getLen();
    final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(path.toString(), 0, len);

    final Map<Integer, List<CacheNode>> indexToSelectedNodes = new HashMap<>(loadingNeeded.size());
    final Map<Integer, HdfsBlockMessage> indexToMsg = new HashMap<>(loadingNeeded.size());

    // For each block that needs loading, synchronously add one location
    final Iterator<Integer> indexIter = loadingNeeded.iterator();
    while (indexIter.hasNext()) {
      final int index = indexIter.next();
      final LocatedBlock locatedBlock = locatedBlocks.get(index);
      final BlockInfo blockInfo = fileMeta.getBlocks().get(index); // TODO: this could cause index out of bounds, when run concurrently with removeLocations!

      // TODO: contains() will be inefficient if blockInfo.getLocations is large
      final List<CacheNode> nodesToChooseFrom = new ArrayList<>(cacheNodes);
      final Iterator<CacheNode> nodeIter = nodesToChooseFrom.iterator();
      while (nodeIter.hasNext()) {
        final CacheNode node = nodeIter.next();
        if (blockInfo.getLocations().contains(new NodeInfo(node.getAddress(), node.getRack()))) { // TODO: this could cause index out of bounds, when run concurrently with removeLocations!
          nodeIter.remove();
        }
      }

      final List<LocatedBlock> locatedBlockList = new ArrayList<>(1);
      locatedBlockList.add(locatedBlock);
      final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(
              locatedBlockList, nodesToChooseFrom, replicationFactor - blockInfo.getLocationsSize()); // TODO: this could cause null pointer, when run concurrently with removeLocations!

      final List<CacheNode> selectedNodes = selected.get(locatedBlock);
      indexToSelectedNodes.put(index, selectedNodes);

      if (selectedNodes.size() == 0) {
        throw new IOException("Surf selected zero caches out of " + cacheNodes.size() + " total caches");
      }

      final HdfsBlockId hdfsBlockId = blockFactory.newBlockId(blockInfo);
      final List<HdfsDatanodeInfo> hdfsDatanodeInfos =
              HdfsDatanodeInfo.copyDatanodeInfos(locatedBlock.getLocations());
      final HdfsBlockMessage msg = new HdfsBlockMessage(hdfsBlockId, hdfsDatanodeInfos, pin);
      indexToMsg.put(index, msg);

      if (blockInfo.getLocationsSize() == 0) { // TODO: this could cause null pointer, when run concurrently with removeLocations!
        final CacheNode nodeToAddSync = selectedNodes.remove(0);
        cacheMessenger.addBlock(nodeToAddSync.getTaskId(), msg);

        final NodeInfo location = new NodeInfo(nodeToAddSync.getAddress(), nodeToAddSync.getRack());
        blockInfo.addToLocations(location);
      }

      if (selectedNodes.size() == 0) {
        indexIter.remove();
      }
    }

    // Return the new metadata. If there are still blocks that need replicas,
    // then asynchronously add those locations.
    if (loadingNeeded.size() == 0) {
      return Futures.immediateFuture(fileMeta);
    } else {
      final ListenableFutureTask<FileMeta> task = ListenableFutureTask.create(new Callable<FileMeta>() {
        @Override
        public FileMeta call() throws Exception {

          synchronized (asyncReloadInProgress) {
            if (asyncReloadInProgress.contains(fileMeta)) {
              return fileMeta; // Only one update is needed
            } else {
              asyncReloadInProgress.add(fileMeta);
            }
          }

          // To avoid concurrent operation with sync reload operations, create a new FileMeta.
          // (These async operations will never run concurrently because of the synchronized blocks.)
          final FileMeta newFileMeta = fileMeta.deepCopy();
          for (final int index : loadingNeeded) {
            final BlockInfo blockInfo = newFileMeta.getBlocks().get(index); // TODO: this could cause index out of bounds, when run concurrently with removeLocations!

            final List<CacheNode> selectedNodes = indexToSelectedNodes.get(index);
            final HdfsBlockMessage msg = indexToMsg.get(index);

            for (final CacheNode nodeToAddAsync : selectedNodes) {
              cacheMessenger.addBlock(nodeToAddAsync.getTaskId(), msg);

              final NodeInfo location = new NodeInfo(nodeToAddAsync.getAddress(), nodeToAddAsync.getRack());
              blockInfo.addToLocations(location);
            }
          }

          synchronized (asyncReloadInProgress) {
            asyncReloadInProgress.remove(fileMeta);
            return newFileMeta;
          }
        }
      });

      executor.execute(task);
      return task;
    }
  }

  @Override
  public void close() throws Exception {
    executor.shutdownNow();
    dfsClient.close();
  }
}
