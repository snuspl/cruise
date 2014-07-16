package org.apache.reef.inmemory.common;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.reef.webserver.HttpServer;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredImpl;
import com.microsoft.tang.formats.RequiredParameter;
import com.microsoft.wake.StageConfiguration;
import org.apache.reef.inmemory.driver.*;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheLoader;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheMessenger;
import org.apache.reef.inmemory.driver.hdfs.HdfsCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.hdfs.HdfsRandomCacheSelectionPolicy;
import org.apache.reef.inmemory.driver.service.ServiceRegistry;
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
          .bindNamedParameter(StageConfiguration.NumberOfThreads.class, CACHESERVER_LOADING_THREADS)
          .bindNamedParameter(DfsParameters.Type.class, DFS_TYPE)
          .bindNamedParameter(DfsParameters.Address.class, DFS_ADDRESS)
          .bindImplementation(BlockId.class, HdfsBlockId.class)
          .bindImplementation(CacheLoader.class, HdfsCacheLoader.class)
          .bindImplementation(CacheMessenger.class, HdfsCacheMessenger.class)
          .bindImplementation(CacheManager.class, CacheManagerImpl.class)
          .bindImplementation(HdfsCacheSelectionPolicy.class, HdfsRandomCacheSelectionPolicy.class)
          .bindConstructor(LoadingCache.class, LoadingCacheConstructor.class)
          .build();
}
