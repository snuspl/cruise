package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Surf FileSystem Meta Information management
 */
public class SurfMetaManager {
  private final LoadingCache<Path, FileMeta> fsMeta;
  public static String USERS_HOME = "/user";

  public SurfMetaManager() {
    fsMeta = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .build(
              new CacheLoader<Path, FileMeta>() {
                @Override
                public FileMeta load(Path path) throws FileNotFoundException {
                  // TODO: Communicate with HDFS, Task, and get the correct FileMeta
                  throw new FileNotFoundException();
                }
              }
            );
  }

  public List<BlockInfo> getBlocks(Path path, User creator) throws FileNotFoundException, Throwable {
    try {
      FileMeta metadata = fsMeta.get(getAbsolutePath(path, creator));
      return metadata.getBlocks();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  public long clear() {
    long numEntries = fsMeta.size();
    fsMeta.invalidateAll();
    clearCaches();
    return numEntries;
  }

  private void clearCaches() {
    // TODO: Communicate with Task to clear caches
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
