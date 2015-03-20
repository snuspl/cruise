package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.reef.inmemory.common.hdfs.HdfsFileMetaFactory;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Metadata Loader implementation for HDFS.
 *
 * Invoked by {@link org.apache.reef.inmemory.driver.SurfMetaManager#get}, the CacheLoader
 * gets the file status from HDFS. Metadata is created from the status and then returned
 * to be stored in the LoadingCache.
 */
public final class HdfsMetaLoader extends CacheLoader<Path, FileMeta> implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(HdfsMetaLoader.class.getName());
  private final EventRecorder RECORD;

  private final FileSystem dfs;
  private final HdfsFileMetaFactory metaFactory;

  @Inject
  public HdfsMetaLoader(final FileSystem dfs,
                        final HdfsFileMetaFactory metaFactory,
                        final EventRecorder recorder) {
    this.dfs = dfs;
    this.metaFactory = metaFactory;
    this.RECORD = recorder;
  }

  @Override
  public FileMeta load(final Path path) throws IOException {
    final String pathStr = path.toString();
    LOG.log(Level.INFO, "Load in memory: {0}", pathStr);

    final Event getFileInfoEvent = RECORD.event("driver.get-file-info", pathStr).start();
    final FileStatus fileStatus = dfs.getFileStatus(path);
    if (fileStatus == null) {
      throw new java.io.FileNotFoundException();
    }
    RECORD.record(getFileInfoEvent.stop());

    final FileMeta fileMeta = metaFactory.toFileMeta(fileStatus);
    return fileMeta;
  }

  @Override
  public void close() throws Exception {
    dfs.close();
  }
}
