package org.apache.reef.inmemory.driver;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.task.BlockId;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
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
  private final DFSClient dfsClient;

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex,
                         final CacheMessenger cacheMessenger,
                         final CacheLocationRemover cacheLocationRemover,
                         final CacheUpdater cacheUpdater,
                         final BlockIdFactory blockIdFactory,
                         final LocationSorter locationSorter,
                         final DFSClient dfsClient) {
    this.metadataIndex = metadataIndex;
    this.cacheMessenger = cacheMessenger;
    this.cacheLocationRemover = cacheLocationRemover;
    this.cacheUpdater = cacheUpdater;
    this.blockIdFactory = blockIdFactory;
    this.locationSorter = locationSorter;
    this.dfsClient = dfsClient;
  }

  /**
   * Retrieve metadata of the file.
   * @return {@code null} if the metadata is not found.
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
   * Update the change of metadata (e.g. Added blocks while writing)
   * If the path not exist in the cache, then create an entry with the path.
   * @param fileMeta Metadata to update
   */
  public void update(FileMeta fileMeta) {
    final Path absolutePath = getAbsolutePath(new Path(fileMeta.getFullPath()), fileMeta.getUser());
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
   * Register a file or directory to BaseFS. We can make sure files
   * with same name exist both in Surf and BaseFS.
   * @return {@code true} if the file is created successfully.
   * @throws IOException
   */
  // TODO: Use Permission Properly
  public boolean registerToBaseFS(final FileMeta fileMeta) throws IOException {
    // Create the file or directory to the BaseFS. It throws an IOException if failure occurs.
    if (fileMeta.isDirectory()) {
      return dfsClient.mkdirs(fileMeta.getFullPath(), FsPermission.getDirDefault(), true);
    } else {
      // Use default buffer size configured in the server. Progressable and ChecksumOpt are not used.
      int bufferSize = dfsClient.getServerDefaults().getFileBufferSize();
      dfsClient.create(fileMeta.getFullPath(), FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
              fileMeta.getReplication(), fileMeta.getBlockSize(), null, bufferSize, null);
      return true;
    }
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
    update(meta);
  }

  private Path getAbsolutePath(Path path, User creator) {
    Path newPath;

    if (path.isAbsolute()) {
      newPath = path;
    }
    else {
      newPath = new Path(SurfMetaManager.USERS_HOME + Path.SEPARATOR + creator.getOwner() + Path.SEPARATOR + path);
    }

    return newPath;
  }

  /**
   * Create an entry for file, and write the metadata into both BaseFS and Surf.
   * <p>
   * First, it tries to create a directory in the BaseFS, and the metadata will be updated
   * only when successful.
   * </p>
   * <p>
   * If a failure occurs during this step, there will be no update in the metadata;
   * An exception is thrown or {@code false} is returned.
   * </p>
   * @throws IOException
   */
  public boolean registerFile(String path, short replication, long blockSize) throws IOException {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path);
    fileMeta.setFileSize(0);
    fileMeta.setDirectory(false);
    fileMeta.setReplication(replication);
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setUser(new User()); // TODO User in Surf should be specified properly.

    final boolean isSuccess = registerToBaseFS(fileMeta);
    if (isSuccess) {
      update(fileMeta);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Create an entry for directory, and write the metadata into both BaseFS and Surf.
   * <p>
   * First, it tries to create a directory in the BaseFS, and the metadata will be updated
   * only when successful.
   * </p>
   * <p>
   * If a failure occurs during this step, there will be no update in the metadata;
   * An exception is thrown or {@code false} is returned.
   * </p>
   * @throws IOException
   */
  public boolean registerDirectory(final String path) throws IOException {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path);
    fileMeta.setFileSize(0);
    fileMeta.setDirectory(true);
    fileMeta.setReplication((short)0);
    fileMeta.setBlockSize(0);
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setUser(new User()); // TODO User in Surf should be specified properly.

    final boolean isSuccess = registerToBaseFS(fileMeta);
    if (isSuccess) {
      update(fileMeta);
      return true;
    } else {
      return false;
    }
  }
}
