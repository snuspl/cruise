package org.apache.reef.inmemory.client;

import java.io.IOException;

/**
 * Resolves the address of the Surf Metadata Server
 */
public interface MetaserverResolver {
  /**
   * Get resolved address
   */
  String getAddress() throws IOException;
}
