package org.apache.reef.inmemory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.reef.task.Task;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
public class InMemoryTask implements Task {
  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());

  private boolean isDone = false;
  private Cache<Object, Object> cache = null;

  /**
   * Constructor. Build a cache.
   * lock is an Object for synchronization
   */
  @Inject
  InMemoryTask() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(100L)
        .expireAfterAccess(10, TimeUnit.HOURS)
        .concurrencyLevel(4)
        .build();
  }

  /**
   * Wait until receiving a signal.
   * TODO notify this and set isDone to be true to wake up
   */
  @Override
  public byte[] call(byte[] arg0) throws Exception {
    final ObjectSerializableCodec<String> codec = new ObjectSerializableCodec<>();

    final String message = "Done";
    loadCache();
    while(true) {
      synchronized (this) {
        this.wait();
        if(this.isDone)
          break;
      }
    }
    return codec.encode(message);
  }

  /**
   * Load files to cache
   */
  private void loadCache(){
    File files = new File("/tmp");

    cache.put("total",files.getTotalSpace());
    cache.put("avail",files.getUsableSpace());
    cache.put("used",files.getTotalSpace()-files.getUsableSpace());
    LOG.info("total:" + cache.getIfPresent("total") + "\t"
        + "avail:" + cache.getIfPresent("avail") + "\t"
        + "used:" + cache.getIfPresent("used") + "\n");
  }
}