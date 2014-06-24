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

  @NamedParameter(doc = "InMemory Cache timeout", short_name = "cache_timeout", default_value = "30000")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of threads assigned to the stage", short_name = "num_threads", default_value = "3")
  public static class NumThreads implements Name<Integer>{
  }
}
