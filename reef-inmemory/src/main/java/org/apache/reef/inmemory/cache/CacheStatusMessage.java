package org.apache.reef.inmemory.cache;

import com.google.common.cache.CacheStats;

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
