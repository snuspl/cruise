package org.apache.reef.inmemory.common;

import org.apache.reef.inmemory.common.instrumentation.InstrumentationConfiguration;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;
import org.apache.reef.inmemory.common.instrumentation.ganglia.GangliaConfiguration;
import org.apache.reef.inmemory.common.instrumentation.ganglia.GangliaParameters;
import org.apache.reef.inmemory.common.instrumentation.log.LogReporterConstructor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class InstrumentorImpl implements Instrumentor {
  private static final Logger LOG = Logger.getLogger(InstrumentorImpl.class.getName());

  private final int reporterPeriod;
  private final String reporterLogLevel;
  private final boolean ganglia;
  private final String gangliaHost;
  private final int gangliaPort;
  private final String gangliaPrefix;

  @Inject
  public InstrumentorImpl(@Parameter(InstrumentationParameters.InstrumentationReporterPeriod.class) final int reporterPeriod,
                          @Parameter(InstrumentationParameters.InstrumentationLogLevel.class) final String reporterLogLevel,
                          @Parameter(GangliaParameters.Ganglia.class) final boolean ganglia,
                          @Parameter(GangliaParameters.GangliaHost.class) final String gangliaHost,
                          @Parameter(GangliaParameters.GangliaPort.class) final int gangliaPort,
                          @Parameter(GangliaParameters.GangliaPrefix.class) final String gangliaPrefix) {
    this.reporterPeriod = reporterPeriod;
    this.reporterLogLevel = reporterLogLevel;
    this.ganglia = ganglia;
    this.gangliaHost = gangliaHost;
    this.gangliaPort = gangliaPort;
    this.gangliaPrefix = gangliaPrefix;
  }

  @Override
  public Configuration getConfiguration() {
    Configuration conf;
    conf = getBaseInstrumentationConfiguration();
    conf = mergeGangliaConfiguration(conf);

    return conf;
  }

  private Configuration getBaseInstrumentationConfiguration() {
    return InstrumentationConfiguration.CONF
            .set(InstrumentationConfiguration.REPORTER_PERIOD, reporterPeriod)
            .set(InstrumentationConfiguration.LOG_LEVEL, reporterLogLevel)
            .set(InstrumentationConfiguration.REPORTER_CONSTRUCTORS, LogReporterConstructor.class)
            .build();
  }

  private Configuration mergeGangliaConfiguration(final Configuration instrumentationConf) {
    if (ganglia) {
      LOG.log(Level.INFO, "Configuring task with Ganglia");
      final Configuration gangliaConf = GangliaConfiguration.CONF
              .set(GangliaConfiguration.GANGLIA, true)
              .set(GangliaConfiguration.GANGLIA_HOST, gangliaHost)
              .set(GangliaConfiguration.GANGLIA_PORT, gangliaPort)
              .set(GangliaConfiguration.GANGLIA_PREFIX, gangliaPrefix)
              .build();
      return Configurations.merge(instrumentationConf, gangliaConf);
    } else {
      return instrumentationConf;
    }
  }
}
