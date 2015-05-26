package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.driver.metatree.MetaTree;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps the task implementation to provide metadata operations.
 */
public final class SurfMetaManager {
  private static final Logger LOG = Logger.getLogger(SurfMetaManager.class.getName());

  private final CacheNodeMessenger cacheNodeMessenger;
  private final CacheLocationRemover cacheLocationRemover;
  private final FileMetaUpdater fileMetaUpdater;
  private final MetaTree metaTree;

  @Inject
  public SurfMetaManager(final CacheNodeMessenger cacheNodeMessenger,
                         final CacheLocationRemover cacheLocationRemover,
                         final FileMetaUpdater fileMetaUpdater,
                         final BlockMetaFactory blockMetaFactory,
                         final MetaTree metaTree) {
    this.cacheNodeMessenger = cacheNodeMessenger;
    this.cacheLocationRemover = cacheLocationRemover;
    this.fileMetaUpdater = fileMetaUpdater;
    this.metaTree = metaTree;
  }

  public FileMeta getOrLoadFileMeta(final String path) throws IOException {
    final FileMeta fileMeta = this.metaTree.getOrLoadFileMeta(path);
    return this.fileMetaUpdater.update(path, fileMeta);
  }

  public boolean exists(final String path) {
    return this.metaTree.exists(path);
  }

  public FileMetaStatus getFileMetaStatus(final String path) throws IOException {
    return this.metaTree.getFileMetaStatus(path);
  }

  public List<FileMetaStatus> listFileMetaStatus(final String path) throws IOException {
    return this.metaTree.listFileMetaStatus(path);
  }

  public FileMeta getFileMeta(final String path) throws IOException {
    return this.metaTree.getFileMeta(path);
  }

  public void create(final String path, final long blockSize, final short baseFsReplication) throws IOException {
    this.metaTree.createFile(path, blockSize, baseFsReplication);
  }

  public boolean mkdirs(final String path) throws IOException {
    return this.metaTree.mkdirs(path);
  }

  public boolean rename(final String src, final String dst) throws IOException {
    return this.metaTree.rename(src, dst);
  }

  public boolean remove(final String path, final boolean recursive) throws IOException {
    final Map<NodeInfo, List<BlockId>> nodeToBlocks = getBlockIdsGroupByCacheNode(path);

    final boolean removedFromMetaTree = this.metaTree.remove(path, recursive);
    if (removedFromMetaTree) {
      cacheNodeMessenger.deleteBlocks(nodeToBlocks);
    }
    return removedFromMetaTree;
  }

  /**
   * Get the block ids of a file.
   * @param path The path to delete.
   * @return Block ids grouped by Cache node. Returns an empty mapping if there is nothing to delete.
   */
  private Map<NodeInfo, List<BlockId>> getBlockIdsGroupByCacheNode(final String path) {
    final Map<NodeInfo, List<BlockId>> nodeToBlocks = new HashMap<>();

    try {
      final FileMeta fileMeta = this.metaTree.getFileMeta(path);
      final List<BlockMeta> blocks = fileMeta.getBlocks();
      for (final BlockMeta blockMeta : blocks) {
        for (final NodeInfo nodeInfo : blockMeta.getLocations()) {
          if (!nodeToBlocks.containsKey(nodeInfo)) {
            nodeToBlocks.put(nodeInfo, new ArrayList<BlockId>());
          }
          final List<BlockId> blockIds = nodeToBlocks.get(nodeInfo);
          blockIds.add(new BlockId(blockMeta));
        }
      }
    } catch (IOException e) {
      // When an entry does not exist or the entry is a directory. We do not have to delete any block.
    }
    return nodeToBlocks;
  }

  /**
   * Clear all cached FileMetas
   * @return number of FileMetas cleared
   */
  public long clear() {
    long numEntries = metaTree.unCacheAll();
    cacheNodeMessenger.clearAll();
    return numEntries;
  }

  /**
   * Apply updates from a cache node.
   * Synchronized on the cache, so that only a single set of updates
   * can be applied at once for the same cache.
   */
  public void applyCacheNodeUpdates(final CacheNode cache, final CacheUpdates updates) {
    synchronized (cache) {
      final String address = cache.getAddress();

      for (final CacheUpdates.Failure failure : updates.getFailures()) {
        if (failure.getThrowable() instanceof OutOfMemoryError) {
          LOG.log(Level.SEVERE, "Block loading failure: " + failure.getBlockId(), failure.getThrowable());
          cache.setStopCause(failure.getThrowable().getClass().getName() + " : " + failure.getThrowable().getMessage());
        } else {
          LOG.log(Level.WARNING, "Block loading failure: " + failure.getBlockId(), failure.getThrowable());
        }
        final BlockId blockId = failure.getBlockId();
        cacheLocationRemover.remove(blockId.getFileId(), blockId, address);
      }

      for (final BlockId removed : updates.getRemovals()) {
        LOG.log(Level.INFO, "Block removed: " + removed);
        cacheLocationRemover.remove(removed.getFileId(), removed, address);
      }

      for (final CacheUpdates.Addition addition : updates.getAddition()) {
        final BlockId blockId = addition.getBlockId();
        final long nWritten = addition.getLength();
        this.metaTree.addNewWrittenBlockToFileMetaInTree(blockId, nWritten, cache);
      }
    }
  }
}
