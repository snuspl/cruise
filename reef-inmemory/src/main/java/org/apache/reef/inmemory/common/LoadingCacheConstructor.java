package org.apache.reef.inmemory.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.tang.ExternalConstructor;

import javax.inject.Inject;

/**
 * Constructs an instance of the Guava LoadingCache, for use by Tang injector.
 */
public final class LoadingCacheConstructor implements ExternalConstructor<LoadingCache> {

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
