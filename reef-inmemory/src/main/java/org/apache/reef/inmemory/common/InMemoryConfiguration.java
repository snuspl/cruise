package org.apache.reef.inmemory.common;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.OptionalParameter;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.StageConfiguration;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.hdfs.*;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicyImpl;
import org.apache.reef.inmemory.task.*;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;

/**
 * Builder that creates a Configuration Module to be used at the Driver, based on underlying FS type
 */
public final class InMemoryConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> METASERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> INIT_CACHE_SERVERS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> DEFAULT_MEM_CACHE_SERVERS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_SERVER_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_LOADING_THREADS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHE_MEMORY_SIZE = new RequiredParameter<>();
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

  private static final ConfigurationModule HDFS_CONF = new InMemoryConfiguration()
          .bindNamedParameter(MetaServerParameters.Port.class, METASERVER_PORT)
          .bindNamedParameter(MetaServerParameters.InitCacheServers.class, INIT_CACHE_SERVERS)
          .bindNamedParameter(MetaServerParameters.DefaultMemCacheServers.class, DEFAULT_MEM_CACHE_SERVERS)
          .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
          .bindNamedParameter(CacheParameters.NumServerThreads.class, CACHESERVER_SERVER_THREADS)
          .bindNamedParameter(CacheParameters.Memory.class, CACHE_MEMORY_SIZE)
          .bindNamedParameter(StageConfiguration.NumberOfThreads.class, CACHESERVER_LOADING_THREADS)
          .bindNamedParameter(MetaServerParameters.ReplicationRulesJson.class, REPLICATION_RULES)
          .bindNamedParameter(DfsParameters.Type.class, DFS_TYPE)
          .bindNamedParameter(DfsParameters.Address.class, DFS_ADDRESS)
          .bindImplementation(BlockId.class, HdfsBlockId.class)
          .bindImplementation(CacheLoader.class, HdfsCacheLoader.class)
          .bindImplementation(CacheMessenger.class, HdfsCacheMessenger.class)
          .bindImplementation(CacheManager.class, CacheManagerImpl.class)
          .bindImplementation(HdfsCacheSelectionPolicy.class, HdfsMemoryRemainingSelectionPolicy.class)
          .bindImplementation(ReplicationPolicy.class, ReplicationPolicyImpl.class)
          .bindConstructor(LoadingCache.class, LoadingCacheConstructor.class)
          .build();
}
