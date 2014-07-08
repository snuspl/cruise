package org.apache.reef.inmemory.common;

import java.io.Serializable;

/**
 * Task sends this message to report the status of Cache to Driver
 */
public final class CacheStatusMessage implements Serializable {

  // TODO: Add memory usage
  private final int bindPort;

  public CacheStatusMessage(final int bindPort) {
    this.bindPort = bindPort;
  }

  public int getBindPort() {
    return bindPort;
  }
}
