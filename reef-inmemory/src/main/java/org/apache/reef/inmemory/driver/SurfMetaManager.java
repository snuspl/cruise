package org.apache.reef.inmemory.driver;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.task.BlockId;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.Iterator;
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
  public static String USERS_HOME = "/user";

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex,
                         final CacheMessenger cacheMessenger) {
    this.metadataIndex = metadataIndex;
    this.cacheMessenger = cacheMessenger;
  }

  /**
   * Retreive metadata of the file at the path.
   * This will load the file if it has not been loaded.
   */
  public FileMeta getFile(Path path, User creator) throws FileNotFoundException, Throwable {
    try {
      final Path absolutePath = getAbsolutePath(path, creator);
      metadataIndex.refresh(absolutePath);
      final FileMeta metadata = metadataIndex.get(getAbsolutePath(path, creator));
      return metadata;
    } catch (ExecutionException e) {
      throw e.getCause();
    }
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

  private Path getAbsolutePath(Path path, User creator) {
    Path newPath = null;

    if (path.isAbsolute())
      newPath = path;
    else
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getId() + Path.SEPARATOR + path);

    return newPath;
  }

  /**
   * Apply updates from a cache node.
   * Synchronized on the cache, so that only one a single set of updates
   * can be applied at once from the same cache.
   */
  public void applyUpdates(final CacheNode cache, final CacheUpdates updates) {
    synchronized (cache) {
      final String address = cache.getAddress();
      for (final CacheUpdates.Failure failure : updates.getFailures()) {
        LOG.log(Level.WARNING, "Block loading failure: " + failure.getBlockId(), failure.getThrowable());
        final BlockId blockId = failure.getBlockId();
        removeLocation(address, new Path(blockId.getFilePath()), blockId.getOffset(), blockId.getUniqueId());
      }
      for (final BlockId removed : updates.getRemovals()) {
        LOG.log(Level.INFO, "Block removed: " + removed);
        removeLocation(address, new Path(removed.getFilePath()), removed.getOffset(), removed.getUniqueId());
      }
    }
  }

  private void removeLocation(final String nodeAddress,
                              final Path filePath,
                              final long offset,
                              final long uniqueId) {
    final FileMeta fileMeta = metadataIndex.getIfPresent(filePath);
    if (fileMeta == null) {
      LOG.log(Level.INFO, "FileMeta null for path "+filePath);
      return;
    }
    if (fileMeta.getBlocks() == null) {
      LOG.log(Level.INFO, "FileMeta blocks null for path "+filePath);
      return;
    }

    final BlockInfo blockInfo;
    synchronized (fileMeta) {
      final long blockSize = fileMeta.getBlockSize();
      if (blockSize <= 0) {
        LOG.log(Level.WARNING, "Unexpected block size: "+blockSize);
      }
      final int index = (int) (offset / blockSize);
      if (fileMeta.getBlocksSize() < index) {
        LOG.log(Level.WARNING, "Block index out of bounds: "+index+", "+blockSize);
        return;
      }
      blockInfo = fileMeta.getBlocks().get(index);
    }
    if (blockInfo.getBlockId() != uniqueId) {
      LOG.log(Level.WARNING, "Block IDs did not match: "+blockInfo.getBlockId()+", "+uniqueId);
      return;
    } else if (blockInfo.getLocations() == null) {
      LOG.log(Level.WARNING, "No locations for block "+blockInfo);
      return;
    }

    boolean removed = false;
    synchronized(blockInfo) {
      final Iterator<NodeInfo> iterator = blockInfo.getLocationsIterator();
      while (iterator.hasNext()) {
        final NodeInfo nodeInfo = iterator.next();
        if (nodeInfo.getAddress().equals(nodeAddress)) {
          iterator.remove();
          removed = true;
          break;
        }
      }
    }

    if (removed) {
      LOG.log(Level.INFO, "Removed "+nodeAddress+", "+blockInfo.getLocationsSize()+" locations remaining.");
    } else {
      LOG.log(Level.INFO, "Did not remove "+nodeAddress);
    }
  }
}
