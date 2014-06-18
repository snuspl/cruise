package org.apache.reef.inmemory.cache.hdfs;

import org.apache.reef.inmemory.cache.InMemoryCache;

import javax.inject.Inject;
import java.nio.ByteBuffer;

public final class HdfsBlockLoader {

  private final InMemoryCache cache;

  @Inject
  public HdfsBlockLoader(final InMemoryCache cache) {
    this.cache = cache;
  }

  public void loadOnMessage(HdfsBlockMessage msg) {
    // TODO: Dummy implementation; Transfer data from HDFS NameNode using msg.getLocations()
    ByteBuffer byteBuffer = ByteBuffer.allocate(1);

    cache.put(msg.getBlockId(), byteBuffer);
  }
}
