package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.reef.inmemory.common.FileMetaStatusFactory;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.driver.BaseFsClient;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Client wrapper to work with HDFS.
 */
public class HDFSClient implements BaseFsClient {

  private final DistributedFileSystem dfs;
  private final FileMetaStatusFactory fileMetaStatusFactory;
  private final int bufferSize;

  @Inject
  public HDFSClient(final FileSystem baseFs,
                    final FileMetaStatusFactory fileMetaStatusFactory) throws IOException {
    this.dfs = new DistributedFileSystem();
    this.fileMetaStatusFactory = fileMetaStatusFactory;

    dfs.initialize(baseFs.getUri(), baseFs.getConf());
    bufferSize = dfs.getServerDefaults().getFileBufferSize();
  }

  @Override
  public OutputStream create(final String path, final short replication, final long blockSize) throws IOException {
    return dfs.create(new Path(path), FsPermission.getFileDefault(), EnumSet.of(CreateFlag.CREATE),
            bufferSize, replication, blockSize, null, null);
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

  @Override
  public FileMetaStatus getFileStatus(final String path) throws IOException {
    return fileMetaStatusFactory.newFileMetaStatus(dfs.getFileStatus(new Path(path)));
  }

  @Override
  public List<FileMetaStatus> listStatus(final String path) throws IOException {
    final List<FileMetaStatus> fileMetaStatusList = new ArrayList<>();
    for (final FileStatus fileStatus : dfs.listStatus(new Path(path))) {
      fileMetaStatusList.add(fileMetaStatusFactory.newFileMetaStatus(fileStatus));
    }
    return fileMetaStatusList;
  }
}
