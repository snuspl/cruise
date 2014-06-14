package org.apache.reef.inmemory.fs;

import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Surf FileSystem Meta Information management
 */
public class SurfMetaManager {
  private final LoadingCache<Path, FileMeta> metadataIndex;
  public static String USERS_HOME = "/user";

  @Inject
  public SurfMetaManager(final LoadingCache metadataIndex) {
    this.metadataIndex = metadataIndex;
  }

  public List<BlockInfo> getBlocks(Path path, User creator) throws FileNotFoundException, Throwable {
    try {
      FileMeta metadata = metadataIndex.get(getAbsolutePath(path, creator));
      return metadata.getBlocks();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  public long clear() {
    long numEntries = metadataIndex.size();
    metadataIndex.invalidateAll();
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
