package org.apache.reef.inmemory.driver.hdfs;

import org.apache.reef.inmemory.common.CacheClearMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.CacheMessenger;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.List;

/**
 * Implements HDFS-specific messaging
 */
public final class HdfsCacheMessenger implements CacheMessenger<HdfsBlockMessage> {

  private static final ObjectSerializableCodec<HdfsDriverTaskMessage> CODEC = new ObjectSerializableCodec<>();

  private final CacheManager cacheManager;

  @Inject
  public HdfsCacheMessenger(final CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  @Override
  public void clear(final String taskId) {
    final CacheNode node = cacheManager.getCache(taskId);
    if (node != null) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.clearMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void clearAll() {
    final List<CacheNode> nodes = cacheManager.getCaches();
    for (final CacheNode node : nodes) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.clearMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void addBlock(final String taskId, final HdfsBlockMessage msg) {
    final CacheNode node = cacheManager.getCache(taskId);
    if (node != null) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.hdfsBlockMessage(msg)));
    }
  }
}
