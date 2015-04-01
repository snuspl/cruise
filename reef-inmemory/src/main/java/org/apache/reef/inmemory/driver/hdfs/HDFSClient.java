package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.driver.BaseFsClient;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;

/**
 * Client wrapper to work with HDFS.
 */
public class HDFSClient implements BaseFsClient {

  private final DistributedFileSystem dfs;
  private final int bufferSize;

  @Inject
  public HDFSClient(final FileSystem baseFs) throws IOException {
    this.dfs = new DistributedFileSystem();

    dfs.initialize(baseFs.getUri(), baseFs.getConf());
    bufferSize = dfs.getServerDefaults().getFileBufferSize();
  }

  @Override
  public void create(final String path, final short replication, final long blockSize) throws IOException {
    dfs.create(new Path(path), FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
            bufferSize, replication, blockSize, null, null);
    // TODO: we might want to hold onto FSOutputStream later when we implement writing to HDFS
  }

  @Override
  public boolean mkdirs(final String path) throws IOException {
    // Return {@code false} directly if it fails to create directory in BaseFS.
    return dfs.mkdirs(new Path(path), FsPermission.getDirDefault());
  }

  @Override
  public boolean delete(final String path) throws IOException {
    final boolean recursive = true;
    return dfs.delete(new Path(path), recursive);

  }

  @Override
  public boolean exists(final String path) throws IOException {
    return dfs.exists(new Path(path));
  }

  @Override
  public boolean rename(final String src, final String dst) throws IOException {
    return dfs.rename(new Path(src), new Path(dst));
  }

  /**
   * Only "File" is expected for the given path (TODO: allow querying directory)
   */
  @Override
  public FileMeta getFileStatus(final String path) throws IOException {
    final FileStatus fileStatus = dfs.getFileStatus(new Path(path));
    if (fileStatus == null || fileStatus.isDirectory()) {
      throw new FileNotFoundException();
    }
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setBlockSize(fileStatus.getBlockSize());
    fileMeta.setFileSize(fileStatus.getLen());
    return fileMeta;
  }
}
