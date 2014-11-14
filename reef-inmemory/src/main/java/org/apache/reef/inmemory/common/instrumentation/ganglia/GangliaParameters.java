package org.apache.reef.inmemory.common.instrumentation.ganglia;

import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;

public final class GangliaParameters {

  @NamedParameter(doc = "Use Ganglia", short_name = "ganglia", default_value = "false")
  public static final class Ganglia implements Name<Boolean> {
  }

  @NamedParameter(doc = "Ganglia host name", short_name = "ganglia_host", default_value = "localhost")
  public final static class GangliaHost implements Name<String> {
  }

  @NamedParameter(doc = "Ganglia port number", short_name = "ganglia_port", default_value = "8649")
  public final static class GangliaPort implements Name<Integer> {
  }

  @NamedParameter(doc = "Ganglia prefix", short_name = "ganglia_prefix", default_value = "surf")
  public final static class GangliaPrefix implements Name<String> {
  }
}
