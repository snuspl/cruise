package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.IOException;

/**
 * Factory that creates FileMeta; FileMeta is a Thrift data structure,
 * used to identify a file in SurfFS.
 */
public interface FileMetaFactory<FsFileStatus> {
  /**
   * Create a new FileMeta for file. Used to register metadata index
   * when a new file is created in Surf.
   * @param path The absolute path of the file.
   * @param replication Replication factor of the file.
   * @param blockSize Block size of the file.
   * @return Metadata used in Surf.
   */
  public FileMeta newFileMeta(String path, short replication, long blockSize);

  /**
   * Create a new FileMeta for directory. Used to register metadata index
   * when a new directory is created in Surf.
   * @param path The absolute path of the directory.
   * @return Metadata used in Surf.
   */
  public FileMeta newFileMetaForDir(String path);

  /**
   * Create a new FileMeta using the FS-specific FileStatus.
   * Since FileStatus does not contain block information (except block size),
   * additional information(BlockMeta) should be set afterward to access the actual data.
   * @param status FileStatus returned from BaseFS using getFileStatus() or listStatus() .
   * @return Metadata used in Surf.
   */
  public FileMeta toFileMeta(FsFileStatus status) throws IOException;
}
