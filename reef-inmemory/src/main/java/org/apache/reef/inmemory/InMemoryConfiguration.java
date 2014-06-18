package org.apache.reef.inmemory;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.cache.*;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.fs.*;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

/**
 * Builder that creates a Configuration Module to be used at the Driver, based on underlying FS type
 */
public final class InMemoryConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> METASERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();

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
          .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
          .bindNamedParameter(DfsParameters.Type.class, DFS_TYPE)
          .bindNamedParameter(DfsParameters.Address.class, DFS_ADDRESS)
          .bindImplementation(BlockId.class, HdfsBlockId.class)
          .bindImplementation(CacheLoader.class, HdfsCacheLoader.class)
          .bindImplementation(TaskManager.class, HdfsCacheManager.class)
          .bindImplementation(HdfsTaskSelectionPolicy.class, HdfsRandomTaskSelectionPolicy.class)
          .bindConstructor(LoadingCache.class, LoadingCacheConstructor.class)
          .build();
}
