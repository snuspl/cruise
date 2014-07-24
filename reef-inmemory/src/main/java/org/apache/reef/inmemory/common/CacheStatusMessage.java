package org.apache.reef.inmemory.common;

import java.io.Serializable;

/**
 * Task sends this message to report the status of Cache to Driver
 */
public final class CacheStatusMessage implements Serializable {

  /**
   * The port number which the cache is bound to
   */
  private final CacheStatistics statistics;
  private final int bindPort;

  public CacheStatusMessage(final CacheStatistics statistics,
                            final int bindPort) {
    this.statistics = statistics;
    this.bindPort = bindPort;
  }

  public CacheStatistics getStatistics() {
    return statistics;
  }

  /**
   * Retrieves port the Cache bound to
   * @return Port number
   */
  public int getBindPort() {
    return bindPort;
  }
}
