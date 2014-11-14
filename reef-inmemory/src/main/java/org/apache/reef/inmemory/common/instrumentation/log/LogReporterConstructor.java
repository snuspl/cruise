package org.apache.reef.inmemory.common.instrumentation.log;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.microsoft.tang.ExternalConstructor;
import com.microsoft.tang.annotations.Parameter;
import org.apache.reef.inmemory.common.instrumentation.InstrumentationParameters;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Construct a Log reporter. Periodically writes aggregated Event metrics to the Log.
 */
public final class LogReporterConstructor implements ExternalConstructor<ScheduledReporter> {

  private static final Logger LOG = Logger.getLogger(LogReporterConstructor.class.getName());

  private final ScheduledReporter logReporter;

  /**
   * An OutputStream that outputs to Logger on each flush.
   * For use with ConsoleReporter, use a non-autoFlushing PrintStream.
   */
  private class LoggerOutputStream extends ByteArrayOutputStream {

    private Logger LOG;
    private Level level;

    public LoggerOutputStream(final Logger log, final Level level) {
      super();
      this.LOG = log;
      this.level = level;
    }

    @Override
    public void flush() throws IOException {
      final String report = toString();
      reset();

      LOG.log(level, report);
    }
  }

  @Inject
  public LogReporterConstructor(
          final MetricRegistry registry,
          final @Parameter(InstrumentationParameters.InstrumentationLogLevel.class) String logLevel) {

    Level level;
    try {
      level = Level.parse(logLevel);
    } catch (final IllegalArgumentException e) {
      LOG.log(Level.WARNING, "Could not parse LOG Level, setting to OFF", e);
      level = Level.OFF;
    }

    final OutputStream outputStream = new LoggerOutputStream(LOG, level);
    final PrintStream printStream = new PrintStream(outputStream);


    this.logReporter = ConsoleReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .outputTo(printStream)
            .build();
  }

  @Override
  public ScheduledReporter newInstance() {
    return logReporter;
  }
}
