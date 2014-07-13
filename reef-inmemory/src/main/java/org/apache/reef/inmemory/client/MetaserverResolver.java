package org.apache.reef.inmemory.client;

import java.io.IOException;

public interface MetaserverResolver {
  String getAddress() throws IOException;
}
