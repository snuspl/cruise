package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;

public final class InstrumentationConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<ExternalConstructor<ScheduledReporter>> REPORTER_CONSTRUCTORS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> REPORTER_PERIOD = new RequiredParameter<>();
  public static final RequiredParameter<String> LOG_LEVEL = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new InstrumentationConfiguration()
          .bindImplementation(EventRecorder.class, RegisteredEventRecorder.class)
          .bindConstructor(MetricRegistry.class, MetricRegistryConstructor.class)
          .bindNamedParameter(InstrumentationParameters.InstrumentationReporterPeriod.class, REPORTER_PERIOD)
          .bindNamedParameter(InstrumentationParameters.InstrumentationLogLevel.class, LOG_LEVEL)
          .bindSetEntry(InstrumentationParameters.InstrumentationReporters.class, REPORTER_CONSTRUCTORS)
          .build();

  // TODO: use bindSetEntry to get "Reporter" implementations that will be composed (log, metrics, etc.)
}
