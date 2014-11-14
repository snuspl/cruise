package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.formats.ConfigurationModule;
import com.microsoft.tang.formats.ConfigurationModuleBuilder;
import com.microsoft.tang.formats.RequiredParameter;

/**
 * A configuration module for setting up instrumentation
 */
public final class InstrumentationConfiguration extends ConfigurationModuleBuilder {

  /**
   * Each event is logged at the given log level.
   */
  public static final RequiredParameter<String> LOG_LEVEL = new RequiredParameter<>();

  /**
   * Scheduled reporters give periodic aggregate metrics to a sink, e.g., Ganglia, for cluster-wide visibility.
   * Note these are aggregates, done in addition to logging each event individually.
   */
  public static final RequiredParameter<ExternalConstructor<ScheduledReporter>> REPORTER_CONSTRUCTORS = new RequiredParameter<>();

  /**
   * The period for reporting aggregate metrics.
   */
  public static final RequiredParameter<Integer> REPORTER_PERIOD = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new InstrumentationConfiguration()
          .bindImplementation(EventRecorder.class, RegisteredEventRecorder.class)
          .bindConstructor(MetricRegistry.class, MetricRegistryConstructor.class)
          .bindNamedParameter(InstrumentationParameters.InstrumentationReporterPeriod.class, REPORTER_PERIOD)
          .bindNamedParameter(InstrumentationParameters.InstrumentationLogLevel.class, LOG_LEVEL)
          .bindSetEntry(InstrumentationParameters.InstrumentationReporters.class, REPORTER_CONSTRUCTORS)
          .build();
}
