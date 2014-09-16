package org.apache.reef.inmemory.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * Utilities for setting up Integrated Tests
 */
public final class ITUtils {

  public static final String NAMENODE_PORT = System.getProperty("namenodePort");

  public static final FileSystem getHdfs(final Configuration hdfsConfig) throws IOException {
    final FileSystem hdfs = new DistributedFileSystem();
    hdfs.initialize(URI.create("hdfs://localhost:" + NAMENODE_PORT), hdfsConfig);
    return hdfs;
  }
}
