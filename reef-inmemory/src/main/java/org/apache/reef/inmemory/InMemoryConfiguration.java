package org.apache.reef.inmemory;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.cache.BlockId;
import org.apache.reef.inmemory.cache.HdfsBlockId;
import org.apache.reef.inmemory.fs.HdfsCacheLoader;
import org.apache.reef.inmemory.fs.LoadingCacheConstructor;

public class InMemoryConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> METASERVER_PORT = new RequiredParameter<>();
  public static final RequiredParameter<String> DFS_ADDRESS = new RequiredParameter<>();

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  public static final ConfigurationModule HDFS_CONF = new InMemoryConfiguration()
          .bindNamedParameter(Launch.MetaserverPort.class, METASERVER_PORT)
          .bindNamedParameter(Launch.DfsAddress.class, DFS_ADDRESS)
          .bindImplementation(BlockId.class, HdfsBlockId.class)
          .bindImplementation(CacheLoader.class, HdfsCacheLoader.class)
          .bindConstructor(LoadingCache.class, LoadingCacheConstructor.class)
          .build();
}
