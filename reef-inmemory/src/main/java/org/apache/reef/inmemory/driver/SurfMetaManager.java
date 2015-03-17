package org.apache.reef.inmemory.driver;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.FileMetaFactory;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.driver.locality.LocationSorter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
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
  private final static String USERS_HOME = "/user";

  private final LoadingCache<Path, FileMeta> metadataIndex;
  private final CacheMessenger cacheMessenger;
  private final CacheLocationRemover cacheLocationRemover;
  private final CacheUpdater cacheUpdater;
  private final BlockMetaFactory blockMetaFactory;
  private final FileMetaFactory metaFactory;
  private final LocationSorter locationSorter;
  private final BaseFsClient baseFsClient;

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex,
                         final CacheMessenger cacheMessenger,
                         final CacheLocationRemover cacheLocationRemover,
                         final CacheUpdater cacheUpdater,
                         final BlockMetaFactory blockMetaFactory,
                         final FileMetaFactory metaFactory,
                         final LocationSorter locationSorter,
                         final BaseFsClient baseFsClient) {
    this.metadataIndex = metadataIndex;
    this.cacheMessenger = cacheMessenger;
    this.cacheLocationRemover = cacheLocationRemover;
    this.cacheUpdater = cacheUpdater;
    this.blockMetaFactory = blockMetaFactory;
    this.metaFactory = metaFactory;
    this.locationSorter = locationSorter;
    this.baseFsClient = baseFsClient;
  }

  /**
   * Retrieve metadata of the file.
   * @return {@code null} if the metadata is not found.
   * TODO User should be specified properly
   */
  public FileMeta get(final Path path, final User creator) throws Throwable {
    try {
      final Path absolutePath = getAbsolutePath(path, creator);
      final FileMeta meta = metadataIndex.get(absolutePath);
      if (meta == null) {
        throw new FileNotFoundException("The file is not found : " + absolutePath);
      }
      return meta;
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  /**
   * Load the file if it has not been loaded.
   * Further, it will update the file if caches have removed blocks.
   *
   * @return A copy of the returned fileMeta. Return the meta directly
   * if the file is a directory.
   */
  public FileMeta loadData(final FileMeta fileMeta) throws java.io.IOException {
    if (fileMeta.isDirectory()) {
      return fileMeta;
    } else {
      return cacheUpdater.updateMeta(fileMeta);
    }
  }

  public FileMeta sortOnLocation(final FileMeta fileMeta, final String clientHostName) {
    return locationSorter.sortMeta(fileMeta, clientHostName);
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

  /**
   * Create an entry for file, and write the metadata to BaseFS and Surf.
   * <p>
   * First, it tries to create file in BaseFS.
   * If a failure occurs during this step, there will be no update in the metadata.
   * </p>
   * <p>
   * Second, this file is set as a child of parent directory in Surf.
   * </p>
   * @throws IOException
   */
  public boolean createFile(final String path, final short replication, final long blockSize) throws Throwable {
    final FileMeta fileMeta = metaFactory.newFileMeta(path, replication, blockSize);

    // 1. Try to create file in BaseFS.
    try {
      baseFsClient.create(path, replication, blockSize);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to create a file to the BaseFS : " + path, e.getCause());
      throw e.getCause();
    }

    // 2. Set this file as a child of parent directory. If the parent is same, one file waits until the other ends.
    try {
      final FileMeta parentMeta = getParent(fileMeta);
      synchronized (parentMeta) {
        parentMeta.addToChildren(fileMeta.getFullPath());
        update(parentMeta);
      }
      update(fileMeta);
      return true;
    } catch (Throwable e) {
      LOG.log(Level.SEVERE, "Failed to create a file metadata : " + path, e.getCause());
      if (!deleteFromBaseFS(fileMeta)) {
        LOG.log(Level.SEVERE, "Failed to delete from BaseFS : {0}", fileMeta.getFullPath());
      }
      throw e.getCause();
    }
  }

  /**
   * Create an entry for directory, and write the metadata to BaseFS and Surf.
   * <p>
   * First, it tries to create directory in BaseFS.
   * If a failure occurs during this step, there will be no update in the metadata.
   * </p>
   * <p>
   * Second, the parent directories are registered recursively.
   * </p>
   * @throws IOException
   */
  public boolean createDirectory(final String pathStr) throws Throwable {
    try {
      final String lowestAncestorPathStr = getLowestAncestor(pathStr);
      final FileMeta lowestAncestorMeta= get(new Path(lowestAncestorPathStr), new User());
      final List<FileMeta> createdMetas = new ArrayList<>();

      // Lock the ancestor to prevent from concurrent update.
      synchronized (lowestAncestorMeta) {
        // 1. Try to create directory in BaseFS.
        try {
          // Return false directly if it fails to create directory in BaseFS.
          LOG.log(Level.SEVERE, "Write {0} in the base", pathStr);
          final boolean createdAtBaseFs = baseFsClient.mkdirs(pathStr);
          if (!createdAtBaseFs) {
            return false;
          }
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to create a directory to the BaseFS : " + pathStr, e.getCause());
          throw e.getCause();
        }

        try {
          FileMeta childMeta = get(new Path(pathStr), new User());
          while (!lowestAncestorPathStr.equals(childMeta.getFullPath())) {
            FileMeta parentMeta = getParent(childMeta);
            parentMeta.addToChildren(childMeta.getFullPath());

            createdMetas.add(childMeta); // Remember to rollback.
            childMeta = parentMeta;
          }
        } catch (Throwable e) {
          baseFsClient.delete(lowestAncestorPathStr);
          throw e.getCause();
        }

        for (FileMeta createdMeta : createdMetas) {
          update(createdMeta);
        }
        // Finally, update the lowestAncestor which is the root of created directories.
        update(lowestAncestorMeta);
        return true;
      }
    } catch (Throwable throwable) {
      LOG.log(Level.SEVERE, "Failed to create", throwable);
      throw throwable.getCause();
    }
  }

  /**
   * Get metadata of directory's children files. When the children is not set yet,
   * get the list of file status from BaseFS. Incremental changes of BaseFS directory
   * could be dropped.
   *
   * TODO find a better way to synchronize the metadata with BaseFS.
   * @param fileMeta {@code FileMeta} of a directory to look up.
   * @return List of files that exist under the directory.
   * @throws IOException
   */
  public List<FileMeta> getChildren(final FileMeta fileMeta) throws IOException {
    final List<FileMeta> childList = new ArrayList<>();
    final List<String> childPathList = new ArrayList<>();

    if (!fileMeta.isSetChildren()) {
      for (final FileMeta childMeta : baseFsClient.listStatus(fileMeta.getFullPath())) {
        update(childMeta);
        childList.add(childMeta);
        childPathList.add(childMeta.getFullPath());
      }
      fileMeta.setChildren(childPathList);
    } else {
      try {
        for (final String pathStr : fileMeta.getChildren()) {
          final FileMeta childMeta = get(new Path(pathStr), new User(fileMeta.getUser()));
          // TODO User in Surf should be specified properly.
          childList.add(childMeta);
        }
      } catch (Throwable throwable) {
        // TODO This is somewhat over-catch. Refactor SurfMetaManager#get()
        LOG.log(Level.SEVERE, "Failed while getting list of files under : " + fileMeta.getFullPath());
      }
    }
    return childList;
  }

  /**
   * Return the parent's FileMeta
   * @throws Throwable
   */
  protected FileMeta getParent(final FileMeta fileMeta) throws Throwable {
    final Path path = new Path(fileMeta.getFullPath());
    return get(path.getParent(), fileMeta.getUser());
  }

  /**
   * Returns whether the FileMeta indicates the root directory.
   */
  private boolean isRoot(final FileMeta fileMeta) {
    return getAbsolutePath(new Path(fileMeta.getFullPath()), fileMeta.getUser()).isRoot();
  }

  private void addBlockToFileMeta(final BlockId blockId, final long nWritten, final CacheNode cacheNode) {
    final FileMeta meta = metadataIndex.getIfPresent(new Path(blockId.getFilePath()));

    final List<NodeInfo> nodeList = new ArrayList<>();
    nodeList.add(new NodeInfo(cacheNode.getAddress(), cacheNode.getRack()));
    final BlockMeta newBlock = blockMetaFactory.newBlockMeta(blockId, nodeList);

    meta.setFileSize(meta.getFileSize() + nWritten);
    meta.addToBlocks(newBlock);
    update(meta);
  }

  private Path getAbsolutePath(final Path path, final User creator) {
    final Path newPath;

    if (path.isAbsolute()) {
      newPath = path;
    } else {
      newPath = new Path(USERS_HOME + Path.SEPARATOR + creator.getOwner() + Path.SEPARATOR + path);
    }
    return newPath;
  }

  /**
   * Update the change of metadata (e.g. Added blocks while writing)
   * If the path not exist in the cache, then create an entry with the path.
   * @param fileMeta Metadata to update
   */
  private void update(final FileMeta fileMeta) {
    final Path absolutePath = getAbsolutePath(new Path(fileMeta.getFullPath()), fileMeta.getUser());
    metadataIndex.put(absolutePath, fileMeta);
  }

  /**
   * Delete the file from BaseFs.
   * This is used when an Exception occurs while updating parents' metadata.
   */
  private boolean deleteFromBaseFS(final FileMeta fileMeta) {
    try {
      return baseFsClient.delete(fileMeta.getFullPath());
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to delete from BaseFS : {0}", fileMeta.getFullPath());
      return false;
    }
  }

  /**
   * Get the FileMeta of existing lowest ancestor to create a directory in the path.
   */
  protected String getLowestAncestor(final String pathStr) throws IOException {
    Path path = new Path(pathStr);
    while (!baseFsClient.exists(path.toUri().getPath())) {
      path = path.getParent();
    }
    return path.toUri().getPath();
  }
}
