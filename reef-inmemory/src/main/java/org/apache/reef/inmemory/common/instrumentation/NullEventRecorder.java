package org.apache.reef.inmemory.common.instrumentation;

/**
 * An Event Recorder that does not record anything.
 */
public final class NullEventRecorder implements EventRecorder {
  @Override
  public Event event(final String name, final String id) {
    return new EventImpl(name, id);
  }

  @Override
  public void record(final Event event) {
  }
}
