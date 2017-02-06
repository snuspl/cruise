package org.apache.reef.inmemory.client;

/**
 * The Inet resolver simply returns the given address.
 * To be used when Surf Client can specify the Driver at a known Inet address, e.g.
 *   surf://localhost:18000
 */
public final class InetMetaserverResolver implements MetaserverResolver {

  final String metaserverAddress;

  public InetMetaserverResolver(final String metaserverAddress) {
    this.metaserverAddress = metaserverAddress;
  }

  @Override
  public String getAddress() {
    return metaserverAddress;
  }
}
