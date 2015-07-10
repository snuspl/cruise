package org.apache.reef.inmemory.common;

import java.io.Serializable;

/**
 * Task sends this message to report the status of Cache to Driver.
 */
public final class CacheStatusMessage implements Serializable {

  private final CacheStatistics statistics;
  private final CacheUpdates updates;
  /**
   * The port number which the cache is bound to.
   */
  private final int bindPort;

  public CacheStatusMessage(final CacheStatistics statistics,
                            final CacheUpdates updates,
                            final int bindPort) {
    this.statistics = statistics;
    this.updates = updates;
    this.bindPort = bindPort;
  }

  /**
   * @return Statistics received with the message
   */
  public CacheStatistics getStatistics() {
    return statistics;
  }

  /**
   * @return Updates received with the message
   */
  public CacheUpdates getUpdates() {
    return updates;
  }

  /**
   * Retrieves port the Cache bound to.
   * @return Port number received with the message
   */
  public int getBindPort() {
    return bindPort;
  }
}
