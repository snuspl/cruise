package org.apache.reef.inmemory.fs.service;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

/**
 * Parameters used by the Driver Metadata Server.
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

  @NamedParameter(doc = "Default cache replicas", short_name = "num_replicas", default_value = "1")
  public static final class Replicas implements Name<Integer> {
  }
}
