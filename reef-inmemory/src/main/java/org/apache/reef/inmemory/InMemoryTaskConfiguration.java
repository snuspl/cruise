package org.apache.reef.inmemory;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.cache.*;
import org.apache.reef.inmemory.cache.InMemoryCacheImpl;
import org.apache.reef.inmemory.cache.hdfs.HdfsDriverMessageHandler;

/**
 * Builder that creates a Configuration Module to be used at each Task, based on underlying FS type
 */
public final class InMemoryTaskConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> CACHESERVER_PORT = new RequiredParameter<>();

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  private static final ConfigurationModule HDFS_CONF = new InMemoryTaskConfiguration()
          .bindNamedParameter(CacheParameters.Port.class, CACHESERVER_PORT)
          .bindImplementation(InMemoryCache.class, InMemoryCacheImpl.class)
          .bindImplementation(DriverMessageHandler.class, HdfsDriverMessageHandler.class)
          .build();
}
