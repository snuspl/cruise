package org.apache.reef.inmemory.common.instrumentation.ganglia;

import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;

public final class GangliaConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Boolean> GANGLIA = new RequiredParameter<>();
  public static final RequiredParameter<String> GANGLIA_HOST = new RequiredParameter<>();
  public static final RequiredParameter<Integer> GANGLIA_PORT = new RequiredParameter<>();
  public static final RequiredParameter<String> GANGLIA_PREFIX = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new GangliaConfiguration()
          .bindSetEntry(InstrumentationParameters.InstrumentationReporters.class, GangliaReporterConstructor.class)
          .bindNamedParameter(GangliaParameters.Ganglia.class, GANGLIA)
          .bindNamedParameter(GangliaParameters.GangliaHost.class, GANGLIA_HOST)
          .bindNamedParameter(GangliaParameters.GangliaPort.class, GANGLIA_PORT)
          .bindNamedParameter(GangliaParameters.GangliaPrefix.class, GANGLIA_PREFIX)
          .build();

  // TODO: use bindSetEntry to get "Reporter" implementations that will be composed (log, metrics, etc.)
}
