package org.apache.reef.inmemory.driver;

import java.io.IOException;

/**
 * Get block locations from BaseFS. Each implementation can specify the representation of block locations
 * and path.
 * <p>For example, its implementation for HDFS({@link org.apache.reef.inmemory.driver.hdfs.HdfsBlockLocationGetter})
 * uses {@link org.apache.hadoop.fs.Path} and list of {@link org.apache.hadoop.hdfs.protocol.LocatedBlock}
 * respectively.</p>
 */
public interface BlockLocationGetter<FsPath, FsBlockLocations> {
  /**
   * Returns the block locations of file.
   * @param path Path of the file.
   * @return Block locations of the file.
   * @throws IOException
   */
  public FsBlockLocations getBlockLocations(FsPath path) throws IOException;
}
