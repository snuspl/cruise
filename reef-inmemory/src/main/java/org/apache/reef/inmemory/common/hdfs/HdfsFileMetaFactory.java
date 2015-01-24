package org.apache.reef.inmemory.common.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.reef.inmemory.common.FileMetaFactory;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.User;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Factory that creates FileMeta using the FileStatus information used in HDFS.
 */
public class HdfsFileMetaFactory implements FileMetaFactory<FileStatus> {
  @Inject
  public HdfsFileMetaFactory() {
  }

  @Override
  public FileMeta newFileMeta(final String path, final short replication, final long blockSize) {
    final FileMeta meta = new FileMeta();
    meta.setFullPath(path);
    meta.setFileSize(0);
    meta.setDirectory(false);
    meta.setReplication(replication);
    meta.setBlockSize(blockSize);
    meta.setUser(new User()); // TODO User in Surf should be specified properly.
    return meta;
  }

  @Override
  public FileMeta newFileMetaForDir(final String path) {
    final FileMeta meta = new FileMeta();
    meta.setFullPath(path);
    meta.setFileSize(0);
    meta.setDirectory(true);
    meta.setReplication((short) 0);
    meta.setBlockSize(0);
    meta.setUser(new User()); // TODO User in Surf should be specified properly.
    return meta;
  }

  @Override
  public FileMeta toFileMeta(final FileStatus fileStatus) throws IOException {
    final FileMeta fileMeta = new FileMeta();
    final String pathStr = fileStatus.getPath().toUri().getPath();
    fileMeta.setFullPath(pathStr);
    fileMeta.setFileSize(fileStatus.getLen());
    fileMeta.setDirectory(fileStatus.isDirectory());
    fileMeta.setReplication(fileStatus.getReplication());
    fileMeta.setBlockSize(fileStatus.getBlockSize());
    fileMeta.setModificationTime(fileStatus.getModificationTime());
    fileMeta.setAccessTime(fileStatus.getAccessTime());
    fileMeta.setUser(new User(fileStatus.getOwner(), fileStatus.getGroup()));
    // TODO : Do we need to support symlink? Is it used frequently in frameworks?
    if (fileStatus.isSymlink()) {
      fileMeta.setSymLink(fileStatus.getSymlink().toUri().getPath());
    }
    return fileMeta;
  }
}
