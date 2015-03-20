package org.apache.reef.inmemory.driver.hdfs;

import org.apache.reef.inmemory.common.CacheClearMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.driver.CacheNodeManager;
import org.apache.reef.inmemory.driver.CacheNodeMessenger;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.List;

/**
 * Implements HDFS-specific messaging
 */
public final class HdfsCacheNodeMessenger implements CacheNodeMessenger<HdfsBlockMessage> {

  private static final ObjectSerializableCodec<HdfsDriverTaskMessage> CODEC = new ObjectSerializableCodec<>();

  private final CacheNodeManager cacheNodeManager;

  @Inject
  public HdfsCacheNodeMessenger(final CacheNodeManager cacheNodeManager) {
    this.cacheNodeManager = cacheNodeManager;
  }

  @Override
  public void clear(final String taskId) {
    final CacheNode node = cacheNodeManager.getCache(taskId);
    if (node != null) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.clearMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void clearAll() {
    final List<CacheNode> nodes = cacheNodeManager.getCaches();
    for (final CacheNode node : nodes) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.clearMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void addBlock(final String taskId, final HdfsBlockMessage msg) {
    final CacheNode node = cacheNodeManager.getCache(taskId);
    if (node != null) {
      node.send(CODEC.encode(HdfsDriverTaskMessage.hdfsBlockMessage(msg)));
    }
  }
}
