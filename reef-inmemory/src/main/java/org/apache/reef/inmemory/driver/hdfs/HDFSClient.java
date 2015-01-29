package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.hdfs.HdfsFileMetaFactory;
import org.apache.reef.inmemory.driver.BaseFsClient;

import javax.inject.Inject;
import java.io.IOException;
import java.util.EnumSet;

/**
 */
public class HDFSClient implements BaseFsClient<FileStatus> {

  private final DistributedFileSystem dfs;
  private final HdfsFileMetaFactory metaFactory;
  private final int bufferSize;

  @Inject
  public HDFSClient(final FileSystem baseFs,
                    final HdfsFileMetaFactory metaFactory) throws IOException {
    this.dfs = new DistributedFileSystem();
    this.metaFactory = metaFactory;

    dfs.initialize(baseFs.getUri(), baseFs.getConf());
    bufferSize = dfs.getServerDefaults().getFileBufferSize();
  }

  @Override
  public void create(String path, short replication, long blockSize) throws IOException {
    dfs.create(new Path(path), FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
            bufferSize, replication, blockSize, null, null);
  }

  @Override
  public boolean mkdirs(String path) throws IOException {
    // Return {@code false} directly if it fails to create directory in BaseFS.
    return dfs.mkdirs(new Path(path), FsPermission.getDirDefault());
  }

  @Override
  public boolean delete(String path) throws IOException {
    final boolean recursive = true;
    return dfs.delete(new Path(path), recursive);

  }

  @Override
  public FileMeta[] listStatus(String path) throws IOException {
    final FileStatus[] statuses = dfs.listStatus(new Path(path));
    final FileMeta[] metas = new FileMeta[statuses.length];
    for (int i = 0; i < statuses.length; i++) {
      metas[i] = metaFactory.toFileMeta(statuses[i]);
    }
    return metas;
  }
}
