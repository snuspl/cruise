package org.apache.reef.inmemory.fs;

import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;

import javax.inject.Inject;
import java.util.List;

/**
 * Implements HDFS-specific
 * general Task/Cache management methods,
 */

public final class HdfsCacheMessenger implements CacheMessenger<HdfsBlockMessage> {

  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  private final CacheManager cacheManager;

  @Inject
  public HdfsCacheMessenger(final CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  @Override
  public void clear(String taskId) {
    CacheNode node = cacheManager.getCache(taskId);
    if (node != null) {
      node.getTask().send(CODEC.encode(new HdfsMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void clearAll() {
    List<CacheNode> nodes = cacheManager.getCaches();
    for (CacheNode node : nodes) {
      node.getTask().send(CODEC.encode(new HdfsMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void addBlock(String taskId, HdfsBlockMessage msg) {
    CacheNode node = cacheManager.getCache(taskId);
    if (node != null) {
      node.getTask().send(CODEC.encode(new HdfsMessage(msg)));
    }
  }
}
