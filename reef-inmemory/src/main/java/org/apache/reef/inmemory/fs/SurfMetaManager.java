package org.apache.reef.inmemory.fs;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

/**
 * Wraps the cache implementation to provide metadata operations.
 */
public final class SurfMetaManager {
  private final LoadingCache<Path, FileMeta> metadataIndex;
  private final CacheManager cacheManager;
  public static String USERS_HOME = "/user";

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex,
                         final CacheManager cacheManager) {
    this.metadataIndex = metadataIndex;
    this.cacheManager = cacheManager;
  }

  public FileMeta getBlocks(Path path, User creator) throws FileNotFoundException, Throwable {
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
    cacheManager.clearAll();
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
}
