package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.reef.inmemory.common.BlockIdFactory;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.StageConfiguration;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockIdFactory;
import org.apache.reef.inmemory.driver.hdfs.*;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicyImpl;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;
import org.apache.reef.inmemory.driver.write.WritingCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.write.WritingRandomCacheSelectionPolicy;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.CacheParameters;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;

/**
 * Builder that creates a Configuration Module to be used at the Driver, based on underlying FS type
 */
public final class InMemoryDriverConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> METASERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> INIT_CACHE_SERVERS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> DEFAULT_MEM_CACHE_SERVERS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_SERVER_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_LOADING_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHE_MEMORY_SIZE = new RequiredParameter<>();
  public static final RequiredParameter<Double> CACHESERVER_HEAP_SLACK = new RequiredParameter<>();
  public static final OptionalParameter<String> REPLICATION_RULES = new OptionalParameter<>();

  public static final RequiredParameter<String> DFS_TYPE = new RequiredParameter<>();
  public static final RequiredParameter<String> DFS_ADDRESS = new RequiredParameter<>();

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  private static final ConfigurationModule HDFS_CONF = new InMemoryDriverConfiguration()
    .bindNamedParameter(MetaServerParameters.Port.class, METASERVER_PORT)
    .bindNamedParameter(MetaServerParameters.InitCacheServers.class, INIT_CACHE_SERVERS)
    .bindNamedParameter(MetaServerParameters.DefaultMemCacheServers.class, DEFAULT_MEM_CACHE_SERVERS)
    .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
    .bindNamedParameter(CacheParameters.NumServerThreads.class, CACHESERVER_SERVER_THREADS)
    .bindNamedParameter(CacheParameters.Memory.class, CACHE_MEMORY_SIZE)
    .bindNamedParameter(CacheParameters.HeapSlack.class, CACHESERVER_HEAP_SLACK)
    .bindNamedParameter(StageConfiguration.NumberOfThreads.class, CACHESERVER_LOADING_THREADS)
    .bindNamedParameter(MetaServerParameters.ReplicationRulesJson.class, REPLICATION_RULES)
    .bindNamedParameter(DfsParameters.Type.class, DFS_TYPE)
    .bindNamedParameter(DfsParameters.Address.class, DFS_ADDRESS)
    .bindNamedParameter(StageConfiguration.StageHandler.class, TaskMessageHandlerExecutor.class)
    .bindImplementation(BlockId.class, HdfsBlockId.class)
    .bindImplementation(BlockIdFactory.class, HdfsBlockIdFactory.class)
    .bindImplementation(CacheLoader.class, HdfsCacheLoader.class)
    .bindImplementation(CacheMessenger.class, HdfsCacheMessenger.class)
    .bindImplementation(CacheUpdater.class, HdfsCacheUpdater.class)
    .bindImplementation(CacheManager.class, CacheManagerImpl.class)
    .bindImplementation(HdfsCacheSelectionPolicy.class, HdfsRandomCacheSelectionPolicy.class)
    .bindImplementation(WritingCacheSelectionPolicy.class, WritingRandomCacheSelectionPolicy.class)
    .bindImplementation(ReplicationPolicy.class, ReplicationPolicyImpl.class)
    .bindImplementation(EStage.class, ThreadPoolStage.class)
    .bindConstructor(LoadingCache.class, LoadingCacheConstructor.class)
    .build();
}
