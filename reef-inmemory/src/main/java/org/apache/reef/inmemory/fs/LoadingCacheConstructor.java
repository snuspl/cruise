package org.apache.reef.inmemory.fs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.ExternalConstructor;

import javax.inject.Inject;

public class LoadingCacheConstructor implements ExternalConstructor<LoadingCache> {

  private final CacheLoader cacheLoader;

  @Inject
  public LoadingCacheConstructor(final CacheLoader cacheLoader) {
    this.cacheLoader = cacheLoader;
  }

  @Override
  public LoadingCache newInstance() {
    return CacheBuilder.newBuilder()
                    .concurrencyLevel(4)
                    .build(cacheLoader);
  }
}
