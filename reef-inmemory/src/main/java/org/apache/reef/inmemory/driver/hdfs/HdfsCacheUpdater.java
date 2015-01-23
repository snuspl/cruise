package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.CacheLocationRemover;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.CacheUpdater;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HdfsCacheUpdater implements CacheUpdater, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(HdfsCacheUpdater.class.getName());

  private final CacheManager cacheManager;
  private final HdfsCacheMessenger cacheMessenger;
  private final HdfsCacheSelectionPolicy cacheSelector;
  private final CacheLocationRemover cacheLocationRemover;
  private final HdfsBlockIdFactory blockFactory;
  private final ReplicationPolicy replicationPolicy;
  private final DFSClient dfsClient; // Access must be synchronized

  /**
   * @param cacheManager Provides an updated list of caches
   * @param cacheMessenger Provides a channel for block replication messages
   * @param cacheSelector Selects from available caches based on the implemented policy
   * @param cacheLocationRemover Provides the log of pending removals
   * @param blockFactory Translates between block representations
   * @param replicationPolicy Provides the replication policy for each file
   * @param dfsClient DFSClient to access to HDFS
   */
  @Inject
  public HdfsCacheUpdater(final CacheManager cacheManager,
                          final HdfsCacheMessenger cacheMessenger,
                          final HdfsCacheSelectionPolicy cacheSelector,
                          final CacheLocationRemover cacheLocationRemover,
                          final HdfsBlockIdFactory blockFactory,
                          final ReplicationPolicy replicationPolicy,
                          final DFSClient dfsClient) {
    this.cacheManager = cacheManager;
    this.cacheMessenger = cacheMessenger;
    this.cacheSelector = cacheSelector;
    this.cacheLocationRemover = cacheLocationRemover;
    this.blockFactory = blockFactory;
    this.replicationPolicy = replicationPolicy;
    this.dfsClient = dfsClient;
  }

  /**
   * Apply the changes from cache nodes and load blocks if needed to fulfill
   * replication factor.
   *
   * 0. Apply removes
   * 1. Resolve replication policy
   * 2. Get information from HDFS
   * 3. For each block that needs loading, load the block asynchronously
   * 4. Return a copy of the new metadata
   *
   * Other concurrent requests for the same file will block on this update.
   *
   * @param fileMeta Updated in place, using a synchronized block. This should be the single point where FileMeta's are updated.
   * TODO: how bad is the deep copy for performance?
   * @return A deep copy of FileMeta, to prevent modifications until FileMeta is passed to the network.
   * @throws IOException
   */
  @Override
  public FileMeta updateMeta(FileMeta fileMeta) throws IOException {
    synchronized (fileMeta) {
      final Path path = new Path(fileMeta.getFullPath());
      final long blockSize = fileMeta.getBlockSize();

      // 0. Apply removes
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(fileMeta.getFullPath());
      if (pendingRemoves != null) {
        applyRemoves(fileMeta, pendingRemoves);
      }

      // 1. Resolve replication policy
      final List<CacheNode> cacheNodes = cacheManager.getCaches();
      if (cacheNodes.size() == 0) {
        throw new IOException("Surf has zero caches");
      }
      final Action action = replicationPolicy.getReplicationAction(path.toString(), fileMeta);
      final boolean pin = action.getPin();
      final int replicationFactor;
      if (replicationPolicy.isBroadcast(action)) {
        replicationFactor = cacheNodes.size();
      } else {
        replicationFactor = action.getCacheReplicationFactor();
      }

      // 2. Get information from HDFS
      final LocatedBlocks locatedBlocks = getLocatedBlocks(path);

      // 3. For each block that needs loading, load blocks asynchronously
      for (final BlockInfo blockInfo : fileMeta.getBlocks()) {
        final int numLocations = blockInfo.getLocationsSize();
        if (numLocations < replicationFactor) {
          final int index = (int) (blockInfo.getOffSet() / blockSize);
          final LocatedBlock locatedBlock = locatedBlocks.get(index);

          // Filter the nodes to avoid duplicate
          final List<CacheNode> nodesToChooseFrom = filterCacheNodes(cacheNodes, blockInfo);

          final List<LocatedBlock> locatedBlockList = new ArrayList<>(1);
          locatedBlockList.add(locatedBlock);
          final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(
                  locatedBlockList, nodesToChooseFrom, replicationFactor - numLocations);

          final List<CacheNode> selectedNodes = selected.get(locatedBlock);
          if (selectedNodes.size() == 0) {
            throw new IOException("Surf selected zero caches out of " + cacheNodes.size() + " total caches");
          }

          for (final CacheNode nodeToAdd : selectedNodes) {
            final HdfsBlockId hdfsBlockId = blockFactory.newBlockId(blockInfo);
            final List<HdfsDatanodeInfo> hdfsDatanodeInfos =
                    HdfsDatanodeInfo.copyDatanodeInfos(locatedBlock.getLocations());
            final HdfsBlockMessage msg = new HdfsBlockMessage(hdfsBlockId, hdfsDatanodeInfos, pin);
            cacheMessenger.addBlock(nodeToAdd.getTaskId(), msg);
            final NodeInfo location = new NodeInfo(nodeToAdd.getAddress(), nodeToAdd.getRack());
            blockInfo.addToLocations(location);
          }
        }
      }

      // 4. Return a copy of the new metadata
      return fileMeta.deepCopy();
    }
  }

  /**
   * Filter cache nodes to prevent duplicate load by the cache nodes
   * that have the block already.
   * @param cacheNodes Whole cache node list.
   * @param blockInfo The block to load.
   * @return A list of nodes that do not have the block.
   */
  // TODO: contains() will be inefficient if blockInfo.getLocations is large
  private List<CacheNode> filterCacheNodes(List<CacheNode> cacheNodes, BlockInfo blockInfo) {
    final List<CacheNode> nodesToChooseFrom = new ArrayList<>(cacheNodes);
    if (blockInfo.getLocationsSize() > 0) {
      final Iterator<CacheNode> nodeIter = nodesToChooseFrom.iterator();
      while (nodeIter.hasNext()) {
        final CacheNode node = nodeIter.next();
        if (blockInfo.getLocations().contains(new NodeInfo(node.getAddress(), node.getRack()))) {
          nodeIter.remove();
        }
      }
    }
    return nodesToChooseFrom;
  }

  private LocatedBlocks getLocatedBlocks(final Path path) throws IOException {
    synchronized (dfsClient) {
      final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(path.toString());
      if (hdfsFileStatus == null) {
        throw new FileNotFoundException(path.toString());
      }
      final long len = hdfsFileStatus.getLen();
      final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(path.toString(), 0, len);
      return locatedBlocks;
    }
  }

  private List<BlockInfo> applyRemoves(final FileMeta fileMeta, final Map<BlockId, List<String>> pendingRemoves) {
    final List<BlockInfo> blocksWithRemoves = new ArrayList<>(pendingRemoves.size());
    for (final BlockId blockId : pendingRemoves.keySet()) {
      final long offset = blockId.getOffset();
      for (final String nodeAddress : pendingRemoves.get(blockId)) {
        final BlockInfo removed = removeLocation(fileMeta, nodeAddress, offset);
        if (removed != null) {
          blocksWithRemoves.add(removed);
        }
      }
    }
    return blocksWithRemoves;
  }

  private BlockInfo removeLocation(final FileMeta fileMeta,
                                   final String nodeAddress,
                                   final long offset) {
    final BlockInfo blockInfo;
    final long blockSize = fileMeta.getBlockSize();
    if (blockSize <= 0) {
      LOG.log(Level.WARNING, "Unexpected block size: "+blockSize);
    }
    final int index = (int) (offset / blockSize);
    if (fileMeta.getBlocksSize() < index) {
      LOG.log(Level.WARNING, "Block index out of bounds: "+index+", "+blockSize);
      return null;
    }
    blockInfo = fileMeta.getBlocks().get(index);
    if (blockInfo.getOffSet() != offset) {
      LOG.log(Level.WARNING, "The offset did not match: "+blockInfo.getOffSet()+", "+offset);
      return null;
    } else if (blockInfo.getLocations() == null) {
      LOG.log(Level.WARNING, "No locations for block "+blockInfo);
      return null;
    }

    boolean removed = false;
    final Iterator<NodeInfo> iterator = blockInfo.getLocationsIterator();
    while (iterator.hasNext()) {
      final NodeInfo nodeInfo = iterator.next();
      if (nodeInfo.getAddress().equals(nodeAddress)) {
        iterator.remove();
        removed = true;
        break;
      }
    }

    if (removed) {
      LOG.log(Level.INFO, blockInfo.getBlockId()+" removed "+nodeAddress+", "+blockInfo.getLocationsSize()+" locations remaining.");
      return blockInfo;
    } else {
      LOG.log(Level.INFO, "Did not remove "+nodeAddress);
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    dfsClient.close();
  }
}
