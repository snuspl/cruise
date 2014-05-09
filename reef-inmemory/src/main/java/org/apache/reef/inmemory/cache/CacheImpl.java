package org.apache.reef.inmemory.cache;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * Implementation of Cache class using Google Cache interface. 
 */
public class CacheImpl {
  private Cache<String, Object> cache = null;
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  
  /**
   * Constructor of CacheImpl
   */
  public CacheImpl() {
    cache = CacheBuilder.newBuilder()
        .maximumSize(100L)
        .expireAfterAccess(10, TimeUnit.HOURS)
        .concurrencyLevel(4)
        .build();
  }
  
  public byte[] getReport() {
    return CODEC.encode(cache.stats().toString());
  } 
}
