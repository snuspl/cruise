package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockInfoFactory;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMetaFactory;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockInfo;
import org.apache.reef.inmemory.task.hdfs.HdfsDatanodeInfo;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HdfsFileMetaUpdater implements FileMetaUpdater, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(HdfsFileMetaUpdater.class.getName());

  private final CacheNodeManager cacheNodeManager;
  private final HdfsCacheNodeMessenger cacheNodeMessenger;
  private final HdfsCacheSelectionPolicy cacheSelector;
  private final CacheLocationRemover cacheLocationRemover;
  private final HdfsBlockMetaFactory blockMetaFactory;
  private final HdfsBlockInfoFactory blockInfoFactory;
  private final ReplicationPolicy replicationPolicy;
  private final FileSystem dfs; // Access must be synchronized
  private final BlockLocationGetter blockLocationGetter;

  /**
   * @param cacheNodeManager         Provides an updated list of caches
   * @param cacheNodeMessenger       Provides a channel for block replication messages
   * @param cacheSelector        Selects from available caches based on the implemented policy
   * @param cacheLocationRemover Provides the log of pending removals
   * @param replicationPolicy    Provides the replication policy for each file
   * @param dfs                  Client to access to HDFS
   */
  @Inject
  public HdfsFileMetaUpdater(final CacheNodeManager cacheNodeManager,
                             final HdfsCacheNodeMessenger cacheNodeMessenger,
                             final HdfsCacheSelectionPolicy cacheSelector,
                             final CacheLocationRemover cacheLocationRemover,
                             final HdfsBlockMetaFactory blockMetaFactory,
                             final HdfsBlockInfoFactory blockInfoFactory,
                             final ReplicationPolicy replicationPolicy,
                             final FileSystem dfs,
                             final BlockLocationGetter blockLocationGetter) {
    this.cacheNodeManager = cacheNodeManager;
    this.cacheNodeMessenger = cacheNodeMessenger;
    this.cacheSelector = cacheSelector;
    this.cacheLocationRemover = cacheLocationRemover;
    this.blockMetaFactory = blockMetaFactory;
    this.blockInfoFactory = blockInfoFactory;
    this.replicationPolicy = replicationPolicy;
    this.dfs = dfs;
    this.blockLocationGetter = blockLocationGetter;
  }

  /**
   * Apply the changes from cache nodes and load blocks if needed to fulfill
   * replication factor.
   * <p/>
   * 0. Apply removes
   * 1. Resolve replication policy
   * 2. Get information from HDFS
   * 3. For each block that needs loading, load the block asynchronously
   * 4. Return a copy of the new metadata
   * <p/>
   * Other concurrent requests for the same file will block on this update.
   *
   * @param fileMeta Updated in place, using a synchronized block. This should be the single point where FileMeta's are updated.
   * TODO: how bad is the deep copy for performance?
   * @return A deep copy of FileMeta, to prevent modifications until FileMeta is passed to the network.
   * @throws IOException
   */
  @Override
  public FileMeta update(final String path, final FileMeta fileMeta) throws IOException {
    synchronized (fileMeta) {
      final long blockSize = fileMeta.getBlockSize();

      // 0. Apply removes
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(fileMeta.getFileId());
      if (pendingRemoves != null) {
        applyRemoves(fileMeta, pendingRemoves);
      }

      // 1. Resolve replication policy
      final List<CacheNode> cacheNodes = cacheNodeManager.getCaches();
      if (cacheNodes.size() == 0) {
        throw new IOException("Surf has zero caches");
      }
      final Action action = replicationPolicy.getReplicationAction(path, fileMeta);
      final boolean pin = action.getPin();
      final int replicationFactor;
      if (replicationPolicy.isBroadcast(action)) {
        replicationFactor = cacheNodes.size();
      } else {
        replicationFactor = action.getReplication();
      }

      // 2. Get information from HDFS
      assert(blockLocationGetter instanceof HdfsBlockLocationGetter);
      final List<LocatedBlock> locatedBlocks = ((HdfsBlockLocationGetter) blockLocationGetter).getBlockLocations(new Path(path));

      // 3. If the blocks are not resolved in the fileMeta, add them.
      if (fileMeta.getBlocksSize() == 0) {
        for (final LocatedBlock locatedBlock : locatedBlocks) {
          final BlockMeta blockMeta = blockMetaFactory.newBlockMeta(fileMeta.getFileId(), locatedBlock);
          fileMeta.addToBlocks(blockMeta);
        }
      }

      // 3. For each block that needs loading, load blocks asynchronously
      for (final BlockMeta blockMeta : fileMeta.getBlocks()) {
        final BlockId blockId = new BlockId(blockMeta);
        final int numLocations = blockMeta.getLocationsSize();
        if (numLocations < replicationFactor) {
          final int index = (int) (blockMeta.getOffSet() / blockSize);
          final LocatedBlock locatedBlock = locatedBlocks.get(index);

          // Filter the nodes to avoid duplicate
          final List<CacheNode> nodesToChooseFrom = filterCacheNodes(cacheNodes, blockMeta);

          final List<LocatedBlock> locatedBlockList = new ArrayList<>(1);
          locatedBlockList.add(locatedBlock);
          final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(
                  locatedBlockList, nodesToChooseFrom, replicationFactor - numLocations);

          final List<CacheNode> selectedNodes = selected.get(locatedBlock);
          if (selectedNodes.size() == 0) {
            throw new IOException("Surf selected zero caches out of " + cacheNodes.size() + " total caches");
          }

          for (final CacheNode nodeToAdd : selectedNodes) {
            final HdfsBlockInfo hdfsBlockInfo = blockInfoFactory.newBlockInfo(path, locatedBlock);
            final List<HdfsDatanodeInfo> hdfsDatanodeInfos =
                    HdfsDatanodeInfo.copyDatanodeInfos(locatedBlock.getLocations());
            final HdfsBlockMessage msg = new HdfsBlockMessage(blockId, hdfsBlockInfo, hdfsDatanodeInfos, pin);
            cacheNodeMessenger.addBlock(nodeToAdd.getTaskId(), msg);
            final NodeInfo location = new NodeInfo(nodeToAdd.getAddress(), nodeToAdd.getRack());
            blockMeta.addToLocations(location);
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
   * @param blockMeta  The block to load.
   * @return A list of nodes that do not have the block.
   */
  // TODO: contains() will be inefficient if blockMeta.getLocations is large
  private List<CacheNode> filterCacheNodes(List<CacheNode> cacheNodes, BlockMeta blockMeta) {
    final List<CacheNode> nodesToChooseFrom = new ArrayList<>(cacheNodes);
    if (blockMeta.getLocationsSize() > 0) {
      final Iterator<CacheNode> nodeIter = nodesToChooseFrom.iterator();
      while (nodeIter.hasNext()) {
        final CacheNode node = nodeIter.next();
        if (blockMeta.getLocations().contains(new NodeInfo(node.getAddress(), node.getRack()))) {
          nodeIter.remove();
        }
      }
    }
    return nodesToChooseFrom;
  }

  private List<BlockMeta> applyRemoves(final FileMeta fileMeta, final Map<BlockId, List<String>> pendingRemoves) {
    final List<BlockMeta> blocksWithRemoves = new ArrayList<>(pendingRemoves.size());
    for (final BlockId blockId : pendingRemoves.keySet()) {
      final long offset = blockId.getOffset();
      for (final String nodeAddress : pendingRemoves.get(blockId)) {
        final BlockMeta removed = removeLocation(fileMeta, nodeAddress, offset);
        if (removed != null) {
          blocksWithRemoves.add(removed);
        }
      }
    }
    return blocksWithRemoves;
  }

  private BlockMeta removeLocation(final FileMeta fileMeta,
                                   final String nodeAddress,
                                   final long offset) {
    final BlockMeta blockMeta;
    final long blockSize = fileMeta.getBlockSize();
    if (blockSize <= 0) {
      LOG.log(Level.WARNING, "Unexpected block size: "+blockSize);
    }
    final int index = (int) (offset / blockSize);
    if (fileMeta.getBlocksSize() < index) {
      LOG.log(Level.WARNING, "Block index out of bounds: "+index+", "+blockSize);
      return null;
    }
    blockMeta = fileMeta.getBlocks().get(index);
    if (blockMeta.getOffSet() != offset) {
      LOG.log(Level.WARNING, "The offset did not match: "+blockMeta.getOffSet()+", "+offset);
      return null;
    } else if (blockMeta.getLocations() == null) {
      LOG.log(Level.WARNING, "No locations for block "+ blockMeta);
      return null;
    }

    boolean removed = false;
    final Iterator<NodeInfo> iterator = blockMeta.getLocationsIterator();
    while (iterator.hasNext()) {
      final NodeInfo nodeInfo = iterator.next();
      if (nodeInfo.getAddress().equals(nodeAddress)) {
        iterator.remove();
        removed = true;
        break;
      }
    }

    if (removed) {
      LOG.log(Level.INFO, blockMeta.toString()+" removed "+nodeAddress+", "+blockMeta.getLocationsSize()+" locations remaining.");
      return blockMeta;
    } else {
      LOG.log(Level.INFO, "Did not remove "+nodeAddress);
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    dfs.close();
  }
}
