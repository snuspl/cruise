package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.cache.CacheClearMessage;
import org.apache.reef.inmemory.cache.CacheStatusMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements HDFS-specific
 * general Task/Cache management methods,
 */

// TODO: extract all non-HDFS logic to abstract class
public final class HdfsCacheManager implements CacheManager<HdfsBlockMessage> {

  private static final Logger LOG = Logger.getLogger(HdfsCacheManager.class.getName());

  private static final ObjectSerializableCodec<HdfsMessage> CODEC = new ObjectSerializableCodec<>();

  // Tasks are first added to pendingTasks, then moved to tasks after receiving the server port
  private final Map<String, RunningTask> pendingTasks = new HashMap<>();
  private final Map<String, CacheNode> caches = new HashMap<>();

  @Inject
  public HdfsCacheManager() {
    // TODO: Add evaluator request / task submit logic
  }

  @Override
  public synchronized boolean addRunningTask(RunningTask task) {
    if (caches.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      return false;
    } else {
      pendingTasks.put(task.getId(), task);
      LOG.log(Level.INFO, "Add pending task "+task.getId());
      return true;
    }
  }

  @Override
  public synchronized void removeRunningTask(String taskId) {
    caches.remove(taskId);
  }

  @Override
  public synchronized List<CacheNode> getCaches() {
    return new ArrayList<>(caches.values());
  }

  private synchronized CacheNode getCache(String taskId) {
    return caches.get(taskId);
  }

  @Override
  public synchronized void handleUpdate(String taskId, CacheStatusMessage msg) {
    // TODO: eventually, the cache manager should remove Caches that have not been heard from for a long time
    if (pendingTasks.containsKey(taskId) && msg.getBindPort() != 0) {
      final RunningTask task = pendingTasks.remove(taskId);
      final CacheNode cache = new CacheNode(task, msg.getBindPort());
      caches.put(taskId, cache);
      LOG.log(Level.INFO, "Cache "+cache.getAddress()+" added from task "+cache.getTaskId());
    }
  }

  @Override
  public void clear(String taskId) {
    CacheNode node = getCache(taskId);
    if (node != null) {
      node.getTask().send(CODEC.encode(new HdfsMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void clearAll() {
    List<CacheNode> nodes = getCaches();
    for (CacheNode node : nodes) {
      node.getTask().send(CODEC.encode(new HdfsMessage(new CacheClearMessage())));
    }
  }

  @Override
  public void addBlock(String taskId, HdfsBlockMessage msg) {
    CacheNode node = getCache(taskId);
    if (node != null) {
      node.getTask().send(CODEC.encode(new HdfsMessage(msg)));
    }
  }
}
