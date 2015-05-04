package org.apache.reef.inmemory.common;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
/**
 * Parameters for specifying base FS
 */
public final class DfsParameters {
  @NamedParameter(doc = "Base DFS type", short_name = "dfs_type", default_value = "hdfs")
  public static final class Type implements Name<String> {
  }

  @NamedParameter(doc = "Base DFS address", short_name = "dfs_address", default_value = "hdfs://localhost:9000")
  public static final class Address implements Name<String> {
  }
}
