package org.apache.reef.inmemory.driver.hdfs;

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
import java.net.URI;
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
  private final String dfsAddress;
  private final DFSClient dfsClient; // Access must be synchronized

  /**
   * @param cacheManager Provides an updated list of caches
   * @param cacheMessenger Provides a channel for block replication messages
   * @param cacheSelector Selects from available caches based on the implemented policy
   * @param cacheLocationRemover Provides the log of pending removals
   * @param blockFactory Translates between block representations
   * @param replicationPolicy Provides the replication policy for each file
   * @param dfsAddress Address of HDFS, for which a DFSClient connection is made.
   */
  @Inject
  public HdfsCacheUpdater(final CacheManager cacheManager,
                          final HdfsCacheMessenger cacheMessenger,
                          final HdfsCacheSelectionPolicy cacheSelector,
                          final CacheLocationRemover cacheLocationRemover,
                          final HdfsBlockIdFactory blockFactory,
                          final ReplicationPolicy replicationPolicy,
                          final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.cacheManager = cacheManager;
    this.cacheMessenger = cacheMessenger;
    this.cacheSelector = cacheSelector;
    this.cacheLocationRemover = cacheLocationRemover;
    this.blockFactory = blockFactory;
    this.replicationPolicy = replicationPolicy;
    this.dfsAddress = dfsAddress;
    try {
      this.dfsClient = new DFSClient(new URI(this.dfsAddress), new Configuration());
    } catch (Exception ex) {
      throw new RuntimeException("Unable to connect to DFS Client", ex);
    }
  }

  /**
   * 0. Apply removes
   * 1. Resolve replication policy
   * 2. Get information from HDFS
   * 3. For each block that needs loading, synchronously add one location
   * 4. Return a copy of the new metadata
   * TODO: 5. If there are still blocks that need replicas,
   * TODO:    then asynchronously add those locations.
   *
   * Steps 1-4 are intentionally synchronous to avoid replying with blocks with zero locations.
   * Other concurrent requests for the same file will block on this update.
   * TODO: Step 5 is asynchronous, concurrent updates to the same metadata is avoided by using locking.
   *
   * @param path The file's path
   * @param fileMeta Updated in place, using a synchronized block. This should be the single point where FileMeta's are updated.
   * TODO: how bad is the deep copy for performance?
   * @return A deep copy of FileMeta, to prevent modifications until FileMeta is passed to the network.
   * @throws Exception
   */
  @Override
  public FileMeta updateMeta(final Path path, final FileMeta fileMeta) throws IOException {
    synchronized (fileMeta) {
      if (fileMeta.getBlocksSize() == 0) {
        return fileMeta.deepCopy();
      }

      // 0. Apply removes
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(fileMeta.getFullPath());
      if (pendingRemoves == null) {
        return fileMeta.deepCopy();
      }
      final List<BlockInfo> blocksWithRemoves = applyRemoves(fileMeta, pendingRemoves);

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
        replicationFactor = action.getFactor();
      }

      // 2. Get information from HDFS
      final LocatedBlocks locatedBlocks = getLocatedBlocks(path);

      final Map<Integer, List<CacheNode>> indexToSelectedNodes = new HashMap<>(blocksWithRemoves.size());
      final Map<Integer, HdfsBlockMessage> indexToMsg = new HashMap<>(blocksWithRemoves.size());

      // 3. For each block that needs loading, synchronously add one location
      final List<BlockInfo> blocksWithNoReplicas = getBlocksWithNoReplicas(blocksWithRemoves);
      if (blocksWithNoReplicas.size() > 0) {
        final long blockSize = fileMeta.getBlockSize();
        for (final BlockInfo blockInfo : blocksWithNoReplicas) {
          final int index = (int) (blockInfo.getOffSet() / blockSize);
          final LocatedBlock locatedBlock = locatedBlocks.get(index);

          // TODO: contains() will be inefficient if blockInfo.getLocations is large
          final List<CacheNode> nodesToChooseFrom = new ArrayList<>(cacheNodes);
          final Iterator<CacheNode> nodeIter = nodesToChooseFrom.iterator();
          while (nodeIter.hasNext()) {
            final CacheNode node = nodeIter.next();
            if (blockInfo.getLocations().contains(new NodeInfo(node.getAddress(), node.getRack()))) {
              nodeIter.remove();
            }
          }

          final List<LocatedBlock> locatedBlockList = new ArrayList<>(1);
          locatedBlockList.add(locatedBlock);
          final Map<LocatedBlock, List<CacheNode>> selected = cacheSelector.select(
                  locatedBlockList, nodesToChooseFrom, replicationFactor - blockInfo.getLocationsSize());

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

          final CacheNode nodeToAddSync = selectedNodes.remove(0);
          cacheMessenger.addBlock(nodeToAddSync.getTaskId(), msg);

          final NodeInfo location = new NodeInfo(nodeToAddSync.getAddress(), nodeToAddSync.getRack());
          blockInfo.addToLocations(location);
        }
      }
      // 4. Return a copy of the new metadata
      return fileMeta.deepCopy();
    }
  }

  private LocatedBlocks getLocatedBlocks(Path path) throws IOException {
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

  private List<BlockInfo> getBlocksWithNoReplicas(final List<BlockInfo> blocks) {
    final List<BlockInfo> blocksWithNoReplicas = new ArrayList<>(blocks.size());
    for (final BlockInfo block : blocks) {
      if (block.getLocationsSize() == 0) {
        blocksWithNoReplicas.add(block);
      }
    }
    return blocksWithNoReplicas;
  }

  private List<BlockInfo> applyRemoves(final FileMeta fileMeta, final Map<BlockId, List<String>> pendingRemoves) {
    final List<BlockInfo> blocksWithRemoves = new ArrayList<>(pendingRemoves.size());
    for (final BlockId blockId : pendingRemoves.keySet()) {
      final long offset = blockId.getOffset();
      final long uniqueId = blockId.getUniqueId();
      for (final String nodeAddress : pendingRemoves.get(blockId)) {
        final BlockInfo removed = removeLocation(fileMeta, nodeAddress, offset, uniqueId);
        if (removed != null) {
          blocksWithRemoves.add(removed);
        }
      }
    }
    return blocksWithRemoves;
  }

  private BlockInfo removeLocation(final FileMeta fileMeta,
                                   final String nodeAddress,
                                   final long offset,
                                   final long uniqueId) {
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
    if (blockInfo.getBlockId() != uniqueId) {
      LOG.log(Level.WARNING, "Block IDs did not match: "+blockInfo.getBlockId()+", "+uniqueId);
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
