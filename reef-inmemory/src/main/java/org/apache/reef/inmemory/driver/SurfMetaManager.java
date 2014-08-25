package org.apache.reef.inmemory.driver;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.task.BlockId;

import javax.inject.Inject;
import java.io.FileNotFoundException;
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

  public FileMeta getFile(Path path, User creator) throws FileNotFoundException, Throwable {
    try {
      FileMeta metadata = metadataIndex.get(getAbsolutePath(path, creator));
      return metadata;
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  public long clear() {
    long numEntries = metadataIndex.size();
    metadataIndex.invalidateAll();
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

  public void applyUpdates(final String address, final CacheUpdates updates) {
    // TODO: apply updates to actual metadata cache
    for (final CacheUpdates.Failure failure: updates.getFailures()) {
      LOG.log(Level.WARNING, "Block loading failure: "+failure.getBlockId(), failure.getException());
    }
    for (final BlockId removed : updates.getRemovals()) {
      LOG.log(Level.INFO, "Block removed: "+removed);
    }
  }
}
