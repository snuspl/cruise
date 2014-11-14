package org.apache.reef.inmemory.common.instrumentation;

public interface EventRecorder {
  Event event(String name);
  Event event(String name, String id);
  void record(Event event);

  public final class Null {
    public static synchronized EventRecorder get() {
      return new NullEventRecorder();
    }
  }
}
