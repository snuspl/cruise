package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.reef.inmemory.common.entity.NodeInfo;

import java.io.IOException;
import java.util.List;

/**
 * Keeps track of block loading progress at replicas, and decides which
 * replica to try next when a block is loading.
 *
 * The progress is kept per-block, not globally.
 */
public interface LoadProgressManager {

  /**
   * Initialize.
   * @param addresses List of caches that contain this block
   * @param conf Hadoop configuration
   */
  void initialize(List<NodeInfo> addresses, Configuration conf);

  /**
   * Report block's loading progress.
   * @param address Address of the cache where block is loading
   * @param bytesLoaded Amount of block that has been loaded
   */
  void loadingProgress(String address, long bytesLoaded);

  /**
   * Report block not found.
   * @param address Address of the cache where block is loading
   */
  void notFound(String address);

  /**
   * Report block not found.
   * @param address Address of the cache where block is loading
   */
  void notConnected(String address);

  /**
   * Return the cache that the next block load should be attempted on.
   * @return Address of the cache
   * @throws IOException No more cache candidates exist
   */
  String getNextCache() throws IOException;
}
