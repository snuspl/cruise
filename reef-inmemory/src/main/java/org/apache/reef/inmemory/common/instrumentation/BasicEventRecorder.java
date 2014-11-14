package org.apache.reef.inmemory.common.instrumentation;

import com.microsoft.tang.annotations.Parameter;

import java.util.logging.Level;
import java.util.logging.Logger;

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
  public Event event(final String name) {
    return event(name, "");
  }

  @Override
  public Event event(final String name, final String id) {
    return new EventImpl(name, id);
  }

  @Override
  public void record(final Event event) {
    LOG.log(logLevel, event.toJsonString());
  }
}
