package org.apache.reef.inmemory.common;

import java.io.Serializable;

/**
 * Task sends this message to report the status of Cache to Driver
 */
public final class CacheStatusMessage implements Serializable {

  // TODO: Add memory usage
  /**
   * The port number which the cache is bound to
   */
  private final int bindPort;

  public CacheStatusMessage(final int bindPort) {
    this.bindPort = bindPort;
  }

  /**
   * Retrives port the Cache bound to
   * @return Port number
   */
  public int getBindPort() {
    return bindPort;
  }
}
