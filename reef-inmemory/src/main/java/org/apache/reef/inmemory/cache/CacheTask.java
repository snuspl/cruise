package org.apache.reef.inmemory.cache;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.reef.runtime.common.evaluator.task.TaskStatus;
import com.microsoft.reef.task.Task;

/**
 * InMemory Task. Print a message.
 */
public class CacheTask implements Task {

  private static final Logger LOG = Logger.getLogger(CacheTask.class.getName());
  private Cache<Object, Object> cache = null;
  private TaskStatus status;
  
  @Inject
  CacheTask() {
      cache = CacheBuilder.newBuilder()
              .maximumSize(100L)
              .expireAfterAccess(10, TimeUnit.HOURS)
              .concurrencyLevel(4)
              .build();
  }

  @Override
  public byte[] call(byte[] arg0) throws Exception {
    loadCache();
    return null;
  }


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