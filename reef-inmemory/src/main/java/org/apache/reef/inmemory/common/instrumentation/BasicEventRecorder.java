package org.apache.reef.inmemory.common.instrumentation;

import org.apache.reef.tang.annotations.Parameter;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Basic event recorder that logs events.
 */
public final class BasicEventRecorder implements EventRecorder {
  private static final Logger LOG = Logger.getLogger(BasicEventRecorder.class.getName());
  private final Level logLevel;

  public BasicEventRecorder(@Parameter(InstrumentationParameters.InstrumentationLogLevel.class) String logLevel) {
    Level level;
    try {
      level = Level.parse(logLevel);
    } catch (final IllegalArgumentException e) {
      LOG.log(Level.WARNING, "Could not parse LOG Level, setting to OFF", e);
      level = Level.OFF;
    }
    this.logLevel = level;
  }

  @Override
  public Event event(final String group, final String id) {
    return new EventImpl(group, id);
  }

  @Override
  public void record(final Event event) {
    LOG.log(logLevel, event.toJsonString());
  }
}
