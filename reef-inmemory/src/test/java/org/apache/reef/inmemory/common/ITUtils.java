package org.apache.reef.inmemory.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Utilities for setting up Integrated Tests
 */
public final class ITUtils {

  public static final String NAMENODE_PORT_KEY = "namenodePort";
  public static final String NAMENODE_PORT_DEFAULT = "10000";
  public static final String NAMENODE_PORT = getProperty(NAMENODE_PORT_KEY, NAMENODE_PORT_DEFAULT);

  private static String getProperty(final String key, final String defaultValue) {
    final String property = System.getProperty(key);
    final String value = (property == null) ? defaultValue : property;
    return value;
  }

  /**
   * Returns the test directory.
   * Integration tests must cleanup the test directory
   * and must not write to other parts of HDFS.
   */
  public static String getTestDir() {
    return "/user/"+System.getProperty("user.name");
  }

  /**
   * Returns FileSystem instance of HDFS.
   */
  public static FileSystem getHdfs(final Configuration hdfsConfig) throws IOException {
    final FileSystem hdfs = new DistributedFileSystem();
    hdfs.initialize(URI.create("hdfs://localhost:" + NAMENODE_PORT), hdfsConfig);
    return hdfs;
  }

  /**
   * Returns the address of HDFS NameNode.
   */
  public static String getBaseFsAddress() {
    return "hdfs://localhost:" + NAMENODE_PORT;
  }

  /**
   * Helper method to write a file.
   * @throws IOException
   */
  public static Path writeFile(final FileSystem fs,
                                final String path,
                                final int chunkLength,
                                final int numChunks) throws IOException {
    final Path file = new Path(path);

    final FSDataOutputStream outputStream = fs.create(file);

    final byte[] writeChunk = new byte[chunkLength];
    for (int i = 0; i < numChunks; i++) {
      outputStream.write(writeChunk);
    }
    outputStream.close();

    return file;
  }
}
