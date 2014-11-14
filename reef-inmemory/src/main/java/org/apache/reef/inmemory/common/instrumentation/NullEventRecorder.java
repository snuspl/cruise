package org.apache.reef.inmemory.common.instrumentation;

public final class NullEventRecorder implements EventRecorder {
  @Override
  public Event event(final String name) {
    return event(name, "");
  }

  @Override
  public Event event(final String name, final String id) {
    return new EventImpl("", "");
  }

  @Override
  public void record(final Event event) {
  }
}
