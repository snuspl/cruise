package org.apache.reef.inmemory.task;

import java.io.Serializable;

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
