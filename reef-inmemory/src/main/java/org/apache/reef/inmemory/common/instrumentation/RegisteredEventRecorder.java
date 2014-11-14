package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.microsoft.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Event Recorder that:
 * 1. Logs events as JSON
 * 2. Aggregates events to a MetricRegistry
 * 3. Reports aggregates to all ScheduledReporters
 */
public final class RegisteredEventRecorder implements EventRecorder {

  private static final Logger LOG = Logger.getLogger(RegisteredEventRecorder.class.getName());

  private final MetricRegistry registry;
  private final Level logLevel;

  @Inject
  public RegisteredEventRecorder(
          final MetricRegistry registry,
          final @Parameter(InstrumentationParameters.InstrumentationReporterPeriod.class) int period,
          final @Parameter(InstrumentationParameters.InstrumentationLogLevel.class) String logLevel,
          final @Parameter(InstrumentationParameters.InstrumentationReporters.class) Set<ScheduledReporter> reporters) {
    this.registry = registry;
    Level level;
    try {
      level = Level.parse(logLevel);
    } catch (final IllegalArgumentException e) {
      LOG.log(Level.WARNING, "Could not parse LOG Level, setting to OFF", e);
      level = Level.OFF;
    }
    this.logLevel = level;
    for (final ScheduledReporter reporter : reporters) {
      reporter.start(period, TimeUnit.SECONDS);
    }
  }

  @Override
  public Event event(final String name, final String id) {
    return new EventImpl(name, id);
  }

  @Override
  public void record(final Event event) {
    final Timer timer = registry.timer(event.getName());
    timer.update(event.getDuration(), TimeUnit.NANOSECONDS);

    LOG.log(logLevel, event.toJsonString());
  }
}
