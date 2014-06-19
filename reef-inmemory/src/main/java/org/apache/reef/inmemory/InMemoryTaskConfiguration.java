package org.apache.reef.inmemory;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.EStage;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.impl.ThreadPoolStage;
import org.apache.reef.inmemory.cache.BlockLoader;
import org.apache.reef.inmemory.cache.CacheParameters;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.InMemoryCacheImpl;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockLoader;

/**
 * Builder that creates a Configuration Module to be used at each Task, based on underlying FS type
 */
public final class InMemoryTaskConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> NUM_THREADS = new RequiredParameter<>();

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  private static final ConfigurationModule HDFS_CONF = new InMemoryTaskConfiguration()
          .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
          .bindNamedParameter(CacheParameters.NumThreads.class, NUM_THREADS)
          .bindImplementation(InMemoryCache.class, InMemoryCacheImpl.class)
          .bindImplementation(BlockLoader.class, HdfsBlockLoader.class)
          .bindImplementation(EStage.class, ThreadPoolStage.class)
          .bindNamedParameter(StageConfiguration.NumberOfThreads.class, NUM_THREADS)
          .bindNamedParameter(StageConfiguration.StageHandler.class, InMemoryTask.LoadExecutor.class)
          .build();
}
