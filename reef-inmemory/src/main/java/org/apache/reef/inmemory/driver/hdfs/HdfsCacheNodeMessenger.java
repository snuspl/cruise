package org.apache.reef.inmemory.driver.hdfs;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.BlocksDeleteMessage;
import org.apache.reef.inmemory.common.CacheClearMessage;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.CacheNodeManager;
import org.apache.reef.inmemory.driver.CacheNodeMessenger;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

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

  @Override
  public void deleteBlocks(final Map<NodeInfo, List<BlockId>> nodeToBlocks) {
    for (final CacheNode node : cacheNodeManager.getCaches()) {
      final String address = node.getAddress();
      final String rack = node.getRack();
      final NodeInfo nodeInfo = new NodeInfo(address, rack);
      if (nodeToBlocks.containsKey(nodeInfo)) {
        final BlocksDeleteMessage message = new BlocksDeleteMessage(nodeToBlocks.get(nodeInfo));
        node.send(CODEC.encode(HdfsDriverTaskMessage.deleteMessage(message)));
      }
    }
  }
}
