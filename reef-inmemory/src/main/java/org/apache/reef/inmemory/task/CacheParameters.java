package org.apache.reef.inmemory.task;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Parameters for setting up the per-Task Cache
 */
public final class CacheParameters {
  @NamedParameter(doc = "InMemory Cache port", short_name = "cache_port", default_value = "0") // 0: any port
  public static final class Port implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache memory in MB", short_name = "cache_memory", default_value = "512")
  public static final class Memory implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory Cache timeout", short_name = "cache_timeout", default_value = "30000")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of threads assigned to the Cache server", short_name = "cache_server_threads", default_value = "10")
  public static class NumServerThreads implements Name<Integer>{
  }

  @NamedParameter(doc = "Number of threads assigned to the Block Loading stage", short_name = "cache_loading_threads", default_value = "3")
  public static class NumLoadingThreads implements Name<Integer>{
  }
}
