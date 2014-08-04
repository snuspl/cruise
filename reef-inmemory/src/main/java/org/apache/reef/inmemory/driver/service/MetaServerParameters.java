package org.apache.reef.inmemory.driver.service;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Parameters used by the driver Metadata Server.
 */
public final class MetaServerParameters {
  @NamedParameter(doc = "InMemory MetaServer port", short_name = "metaserver_port", default_value = "18000")
  public static final class Port implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory MetaServer timeout", short_name = "metaserver_timeout", default_value = "30000")
  public static final class Timeout implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory MetaServer threads", short_name = "metaserver_num_threads", default_value = "10")
  public static final class Threads implements Name<Integer> {
  }

  @NamedParameter(doc = "Replication rules", short_name = "replication_rules_json", default_value = "{\"rules\":[],\"default\":{\"factor\":1,\"pin\":false}}")
  public static final class ReplicationRulesJson implements Name<String> {
  }

  @NamedParameter(doc = "Number of task servers to allocate on startup", short_name = "cache_servers_num_init", default_value = "1")
  public static final class InitCacheServers implements Name<Integer> {
  }

  @NamedParameter(doc = "Default memory in MB allocated to task servers", short_name = "cache_servers_default_mem", default_value = "512")
  public static final class DefaultMemCacheServers implements Name<Integer> {
  }
}
