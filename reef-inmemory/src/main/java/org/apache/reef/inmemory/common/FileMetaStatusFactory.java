package org.apache.reef.inmemory.common;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.driver.metatree.Entry;
import org.apache.reef.inmemory.driver.metatree.FileEntry;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;

import javax.inject.Inject;
import java.net.URI;
import java.nio.file.Paths;

public class FileMetaStatusFactory {
  private final ReplicationPolicy replicationPolicy;

  @Inject
  public FileMetaStatusFactory(final ReplicationPolicy replicationPolicy) {
    this.replicationPolicy = replicationPolicy;
  }

  public FileMetaStatus newFileMetaStatus(final String path, final Entry entry) {
    if (entry.isDirectory()) {
      final FileMetaStatus fileMetaStatus = new FileMetaStatus();
      fileMetaStatus.setPath(path);
      fileMetaStatus.setIsdir(false);
      // TODO: we might need to assign other attributes (need to investigate into how HDFS does it)
      return fileMetaStatus;
    } else {
      final FileMeta fileMeta = ((FileEntry)entry).getFileMeta();
      return new FileMetaStatus(
              path,
              fileMeta.getFileSize(),
              false,
              // TODO: might be a good idea to make replication 'short' to be consistent with HDFS
              // TODO: there can be consistency issues (e.g. what if replication factor changes after receiving status?)
              replicationPolicy.getReplicationAction(path, fileMeta).getReplication().shortValue(),
              fileMeta.getBlockSize(),
              0L, // TODO: timestamp
              0L, // TODO: timestamp
              FsPermission.getDefault().toShort(), // TODO: ACL
              null, // TODO: ACL
              null, // TODO: ACL
              null // TODO: Symlink
      );
    }
  }

  public FileMetaStatus newFileMetaStatus(final FileStatus fileStatus) throws java.io.IOException {
    return new FileMetaStatus(
            fileStatus.getPath().toString(),
            fileStatus.getLen(),
            fileStatus.isDirectory(),
            fileStatus.getReplication(),
            fileStatus.getBlockSize(),
            fileStatus.getModificationTime(),
            fileStatus.getAccessTime(),
            fileStatus.getPermission().toShort(),
            fileStatus.getOwner(),
            fileStatus.getGroup(),
            null); // TODO: Symlink
  }
}
