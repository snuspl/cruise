package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Intermediate between SurfMetaManager and BaseFs; methods are primitive operations
 * that SurfMetaManager requests to BaseFs.
 */
public interface BaseFsClient {
  /**
   * Request BaseFs to create a file.
   * @param path the path of the file
   * @param replication block replication
   * @param blockSize maximum block size
   * @throws IOException if the operation failed in the BaseFs.
   */
  public OutputStream create(String path, short replication, long blockSize) throws IOException;

  /**
   * Request BaseFs to create a directory.
   * @param path the path of the directory.
   * @return true if the directory is created successfully.
   * @throws IOException if the operation failed in the BaseFs.
   */
  public boolean mkdirs(String path) throws IOException;

  /**
   * Request BaseFs to delete a file. If the file is a directory,
   * files under the directory are deleted.
   * @param path the path of the file
   * @return true only if the file was removed from the Base Fs.
   * @throws IOException if the operation failed in the BaseFs.
   */
  public boolean delete(String path) throws IOException;

  /**
   * Request BaseFs to check whether a file exists.
   * @param path the path of the file
   * @return true if the file exists.
   * @throws IOException if the operation failed in the BaseFs.
   */
  public boolean exists(String path) throws IOException;

  /**
   * Request BaseFs to rename a file.
   * @param src existing path of the file or the directory
   * @param dst new path
   * @return true if the rename is successful.
   * @throws IOException if the operation failed in the BaseFs.
   */
  public boolean rename(String src, String dst) throws IOException;

  /**
   * Get the status of file from BaseFs.
   * @param path the path of the file
   * @return the status of the file, which is encoded in FileMeta
   * @throws IOException if it failed to get the status from BaseFs or the file is not found.
   */
  public FileMeta getFileStatus(final String path) throws IOException;
}
