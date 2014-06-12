package org.apache.reef.inmemory.cache;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface InMemoryCache {
  public ByteBuffer get(FileBlock fileBlock);
  public void put(FileBlock fileBlock, ByteBuffer buffer);
  public void clear();
  public byte[] getReport();
}
