package org.apache.reef.inmemory.fs;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

public class DfsParameters {
  @NamedParameter(doc = "Underlying DFS type", short_name = "dfs_type", default_value = "hdfs")
  public static final class Type implements Name<String> {
  }

  @NamedParameter(doc = "Underlying DFS address", short_name = "dfs_address", default_value = "hdfs://localhost:50070")
  public static final class Address implements Name<String> {
  }
}
