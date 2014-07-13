package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;

public final class LocalMetaserverResolver implements MetaserverResolver {

  final String metaserverAddress;

  public LocalMetaserverResolver(final String metaserverAddress) {
    this.metaserverAddress = metaserverAddress;
  }

  @Override
  public String getAddress() {
    return metaserverAddress;
  }
}
