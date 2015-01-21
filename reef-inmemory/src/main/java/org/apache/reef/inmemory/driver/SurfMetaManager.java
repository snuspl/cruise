package org.apache.reef.inmemory.driver;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.exceptions.IOException;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.task.BlockId;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps the task implementation to provide metadata operations.
 */
public final class SurfMetaManager {
  private static final Logger LOG = Logger.getLogger(SurfMetaManager.class.getName());

  private final LoadingCache<Path, FileMeta> metadataIndex;
  private final CacheMessenger cacheMessenger;
  private final CacheLocationRemover cacheLocationRemover;
  private final CacheUpdater cacheUpdater;
  private final BlockIdFactory blockIdFactory;
  private final LocationSorter locationSorter;
  public static String USERS_HOME = "/user";

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex,
                         final CacheMessenger cacheMessenger,
                         final CacheLocationRemover cacheLocationRemover,
                         final CacheUpdater cacheUpdater,
                         final BlockIdFactory blockIdFactory,
                         final LocationSorter locationSorter) {
    this.metadataIndex = metadataIndex;
    this.cacheMessenger = cacheMessenger;
    this.cacheLocationRemover = cacheLocationRemover;
    this.cacheUpdater = cacheUpdater;
    this.blockIdFactory = blockIdFactory;
    this.locationSorter = locationSorter;
  }

  /**
   * Retrieve metadata of the file
   */
  public FileMeta getFileMeta(final Path path, final User creator) throws FileNotFoundException, Throwable {
    try {
      final Path absolutePath = getAbsolutePath(path, creator);
      final FileMeta fileMeta = metadataIndex.get(absolutePath);
      return fileMeta;
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  /**
   * Load the file if it has not been loaded.
   * Further, it will update the file if caches have removed blocks.
   *
   * @return A copy of the returned fileMeta
   */
  public FileMeta loadData(final FileMeta fileMeta) throws java.io.IOException {
    return cacheUpdater.updateMeta(fileMeta);
  }

  public FileMeta sortOnLocation(final FileMeta fileMeta, final String clientHostName) {
    return locationSorter.sortMeta(fileMeta, clientHostName);
  }

  /**
   * @return {@code true} if file is cached already with the path as a key.
   * @throws ExecutionException
   */
  public boolean exists(Path path, User creator) {
    final Path absolutePath = getAbsolutePath(path, creator);
    return metadataIndex.getIfPresent(absolutePath) != null;
  }

  /**
   * Update the change of metadata (e.g. Added blocks while writing)
   * If the path not exist in the cache, then create an entry with the path.
   * @param fileMeta Metadata to update
   */
  public void update(FileMeta fileMeta, User creator) {
    final Path absolutePath = getAbsolutePath(new Path(fileMeta.getFullPath()), creator);
    metadataIndex.put(absolutePath, fileMeta);
  }

  /**
   * Clear all cached entries
   * @return number of entries cleared
   */
  public long clear() {
    long numEntries = metadataIndex.size();
    metadataIndex.invalidateAll(); // TODO: this may not be so accurate
    cacheMessenger.clearAll();
    return numEntries;
  }

  /**
   * Apply updates from a cache node.
   * Synchronized on the cache, so that only a single set of updates
   * can be applied at once for the same cache.
   */
  public void applyUpdates(final CacheNode cache, final CacheUpdates updates) {
    synchronized (cache) {
      final String address = cache.getAddress();
      for (final CacheUpdates.Failure failure : updates.getFailures()) {
        if (failure.getThrowable() instanceof OutOfMemoryError) {
          LOG.log(Level.SEVERE, "Block loading failure: " + failure.getBlockId(), failure.getThrowable());
          cache.setStopCause(failure.getThrowable().getClass().getName()+" : "+failure.getThrowable().getMessage());
        } else {
          LOG.log(Level.WARNING, "Block loading failure: " + failure.getBlockId(), failure.getThrowable());
        }
        final BlockId blockId = failure.getBlockId();
        cacheLocationRemover.remove(blockId.getFilePath(), blockId, address);
      }
      for (final BlockId removed : updates.getRemovals()) {
        LOG.log(Level.INFO, "Block removed: " + removed);
        cacheLocationRemover.remove(removed.getFilePath(), removed, address);
      }
      for (final CacheUpdates.Addition addition : updates.getAddition()) {
        final BlockId blockId = addition.getBlockId();
        final long nWritten = addition.getLength();
        addBlockToFileMeta(blockId, nWritten, cache);
      }
    }
  }

  private void addBlockToFileMeta(final BlockId blockId, final long nWritten, final CacheNode cacheNode) {
    final FileMeta meta = metadataIndex.getIfPresent(new Path(blockId.getFilePath()));

    final List<NodeInfo> nodeList = new ArrayList<>();
    nodeList.add(new NodeInfo(cacheNode.getAddress(), cacheNode.getRack()));
    final BlockInfo newBlock = blockIdFactory.newBlockInfo(blockId, nodeList);

    meta.setFileSize(meta.getFileSize() + nWritten);
    meta.addToBlocks(newBlock);
    update(meta, new User());
  }

  private Path getAbsolutePath(Path path, User creator) {
    Path newPath = null;

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

    return newPath;
  }
}
