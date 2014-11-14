package org.apache.reef.inmemory.common.instrumentation.ganglia;

import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;

/**
 * A configuration module for Ganglia-specific parameters
 */
public final class GangliaConfiguration extends ConfigurationModuleBuilder {

  /**
   * Hostname of Ganglia Meta Daemon
   */
  public static final RequiredParameter<String> GANGLIA_HOST = new RequiredParameter<>();

  /**
   * Port of Ganglia Meta Daemon
   */
  public static final RequiredParameter<Integer> GANGLIA_PORT = new RequiredParameter<>();

  /**
   * Prefix for reported Ganglia metric entries
   */
  public static final RequiredParameter<String> GANGLIA_PREFIX = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new GangliaConfiguration()
          .bindSetEntry(InstrumentationParameters.InstrumentationReporters.class, GangliaReporterConstructor.class)
          .bindNamedParameter(GangliaParameters.GangliaHost.class, GANGLIA_HOST)
          .bindNamedParameter(GangliaParameters.GangliaPort.class, GANGLIA_PORT)
          .bindNamedParameter(GangliaParameters.GangliaPrefix.class, GANGLIA_PREFIX)
          .build();
}
