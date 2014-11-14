package org.apache.reef.inmemory.common.instrumentation;

public interface Event {
  public Event start();
  public Event stop();
  public String getName();
  public long getDuration();
  public String toJsonString();
}
