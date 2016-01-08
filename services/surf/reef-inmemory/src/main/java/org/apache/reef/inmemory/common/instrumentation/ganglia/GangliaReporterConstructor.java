package org.apache.reef.inmemory.common.instrumentation.ganglia;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Construct a Ganglia reporter. Periodically reports aggregated Event metrics to Ganglia.
 */
public final class GangliaReporterConstructor implements ExternalConstructor<ScheduledReporter> {
  private static final Logger LOG = Logger.getLogger(GangliaReporterConstructor.class.getName());

  private final ScheduledReporter gangliaReporter;

  @Inject
  public GangliaReporterConstructor(
      final MetricRegistry registry,
      @Parameter(GangliaParameters.GangliaHost.class) final String host,
      @Parameter(GangliaParameters.GangliaPort.class) final int port,
      @Parameter(GangliaParameters.GangliaPrefix.class) final String prefix) {

    final GMetric ganglia;
    ScheduledReporter gReporter;
    try {
      ganglia = new GMetric(host, port, GMetric.UDPAddressingMode.UNICAST, 1);
      gReporter = GangliaReporter.forRegistry(registry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .prefixedWith(prefix)
              .build(ganglia);
      LOG.log(Level.INFO, "GangliaReporter was initialized");
    } catch (IOException e) {
      e.printStackTrace();
      gReporter = null;
      LOG.log(Level.WARNING, "GangliaReporter could not be initialized", e);
    }
    this.gangliaReporter = gReporter;
  }

  @Override
  public ScheduledReporter newInstance() {
    return gangliaReporter;
  }
}
