package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.StageConfiguration;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockLoader;
import org.apache.reef.inmemory.task.hdfs.HdfsDriverMessageHandler;

/**
 * Builder that creates a Configuration Module to be used at each Task, based on underlying FS type
 */
public final class InMemoryTaskConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_SERVER_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_LOADING_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Double> CACHESERVER_HEAP_SLACK = new RequiredParameter<>();

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  private static final ConfigurationModule HDFS_CONF = new InMemoryTaskConfiguration()
          .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
          .bindNamedParameter(CacheParameters.NumServerThreads.class, CACHESERVER_SERVER_THREADS)
          .bindNamedParameter(CacheParameters.HeapSlack.class, CACHESERVER_HEAP_SLACK)
          .bindNamedParameter(StageConfiguration.NumberOfThreads.class, CACHESERVER_LOADING_THREADS)
          .bindNamedParameter(StageConfiguration.StageHandler.class, BlockLoaderExecutor.class)
          .bindImplementation(InMemoryCache.class, InMemoryCacheImpl.class)
          .bindImplementation(BlockLoader.class, HdfsBlockLoader.class)
          .bindImplementation(BlockIdFactory.class, HdfsBlockIdFactory.class)
          .bindImplementation(DriverMessageHandler.class, HdfsDriverMessageHandler.class)
          .bindImplementation(EStage.class, ThreadPoolStage.class)
          .bindConstructor(Cache.class, CacheConstructor.class)
          .build();
}
