package org.apache.reef.inmemory.driver;

import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Intermediate between SurfMetaManager and BaseFS; methods are primitive operations
 * that SurfMetaManager requests to BaseFS.
 */
public interface BaseFsClient {
  public void create(String path, short replication, long blockSize) throws IOException;
  public boolean mkdirs(String path) throws IOException;
  public boolean delete(String path) throws IOException;
  public boolean exists(String path) throws IOException;
  public boolean rename(String src, String dst) throws IOException;
  public FileMeta getFileStatus(final String path) throws IOException;
}
