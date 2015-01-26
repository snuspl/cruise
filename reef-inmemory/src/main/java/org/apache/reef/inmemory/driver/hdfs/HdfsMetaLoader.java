package org.apache.reef.inmemory.driver.hdfs;

import com.google.common.cache.CacheLoader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
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

  @Inject
  public HdfsMetaLoader(final DistributedFileSystem dfs,
                        final HdfsBlockIdFactory blockFactory,
                        final EventRecorder recorder) {
    this.dfs = dfs;
    this.blockFactory = blockFactory;
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

    return getFileMeta(pathStr, fileStatus);
  }

  /**
   * @return FileMeta with the same information of FileStatus retrieved from HDFS
   * @throws IOException
   * TODO: use FSPermission properly
   */
  private FileMeta getFileMeta(final String pathStr, final FileStatus fileStatus) throws IOException {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(pathStr);
    fileMeta.setFileSize(fileStatus.getLen());
    fileMeta.setDirectory(fileStatus.isDirectory());
    fileMeta.setReplication(fileStatus.getReplication());
    fileMeta.setBlockSize(fileStatus.getBlockSize());
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setModificationTime(fileStatus.getModificationTime());
    fileMeta.setAccessTime(fileStatus.getAccessTime());
    fileMeta.setUser(new User(fileStatus.getOwner(), fileStatus.getGroup()));
    // TODO : Do we need to support symlink? Is it used frequently in frameworks?
    if (fileStatus.isSymlink()) {
      fileMeta.setSymLink(fileStatus.getSymlink().toUri().getPath());
    }
    if (!fileStatus.isDirectory()) {
      addBlocks(fileMeta, fileStatus.getLen());
    }
    return fileMeta;
  }

  /**
   * Add blocks to fileMeta
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
