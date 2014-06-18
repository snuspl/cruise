package org.apache.reef.inmemory.cache;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Parameters for setting up the per-Task Cache
 */
public final class CacheParameters {
  @NamedParameter(doc = "InMemory Cache port", short_name = "cache_port", default_value = "18001")
  public static final class Port implements Name<Integer> {
  }
}
