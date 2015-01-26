package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.FileMetaFactory;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
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

  private final DistributedFileSystem dfs;
  private final HdfsBlockIdFactory blockFactory;
  private final FileMetaFactory metaFactory;

  @Inject
  public HdfsMetaLoader(final DistributedFileSystem dfs,
                        final HdfsBlockIdFactory blockFactory,
                        final FileMetaFactory metaFactory,
                        final EventRecorder recorder) {
    this.dfs = dfs;
    this.blockFactory = blockFactory;
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
    if (!fileMeta.isDirectory()) {
      addBlocks(fileMeta, fileStatus.getLen());
    }
    return fileMeta;
  }

  /**
   * Add blocks to fileMeta. Each BlockInfo has information to load the block from DataNode directly.
   * @throws IOException
   */
  private void addBlocks(final FileMeta fileMeta, final long fileLength) throws IOException {
    final String pathStr = fileMeta.getFullPath();
    final LocatedBlocks locatedBlocks = dfs.getClient().getLocatedBlocks(pathStr, 0, fileLength);
    final List<LocatedBlock> locatedBlockList = locatedBlocks.getLocatedBlocks();
    for (final LocatedBlock locatedBlock : locatedBlockList) {
      final BlockInfo blockInfo = blockFactory.newBlockInfo(pathStr, locatedBlock);
      fileMeta.addToBlocks(blockInfo);
    }
  }

  @Override
  public void close() throws Exception {
    dfs.close();
  }
}
