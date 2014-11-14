package org.apache.reef.inmemory.common.instrumentation;

/**
 * An instrumented event. Use in conjuction with an EventRecorder to log and/or aggregate events.
 */
public interface Event {
  /**
   * Save the current system time as the start time
   */
  public Event start();

  /**
   * Save the current system time as the stop time
   */
  public Event stop();

  /**
   * Get the total duration of the event computed as (stop time - start time), in milliseconds
   * @return Duration in milliseconds
   */
  public long getDuration();

  /**
   * The Group, which is the unit for aggregation
   * @return Group
   */
  public String getGroup();

  /**
   * The Id, which differentiates events within the same aggregation Group
   * @return Id
   */
  public String getId();

  /**
   * A JSON representation of the Event. Should be used when logging the Event.
   * @return A Json String
   */
  public String toJsonString();
}
