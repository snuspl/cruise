package org.apache.reef.inmemory;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.cache.BlockId;
import org.apache.reef.inmemory.cache.HdfsBlockId;
import org.apache.reef.inmemory.cache.HdfsCache;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.fs.DfsParameters;
import org.apache.reef.inmemory.fs.HdfsCacheLoader;
import org.apache.reef.inmemory.fs.LoadingCacheConstructor;
import org.apache.reef.inmemory.fs.service.MetaServerParameters;

public class InMemoryTaskConfiguration extends ConfigurationModuleBuilder {

  public static final ConfigurationModule getConf(String dfsType) {
    if ("hdfs".equals(dfsType)) {
      return HDFS_CONF;
    } else {
      throw new RuntimeException("Unknown dfs_type: "+dfsType);
    }
  }

  public static final ConfigurationModule HDFS_CONF = new InMemoryTaskConfiguration()
          .bindImplementation(InMemoryCache.class, HdfsCache.class)
          .build();
}
