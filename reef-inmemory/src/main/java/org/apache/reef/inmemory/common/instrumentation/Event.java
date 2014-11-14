package org.apache.reef.inmemory.common.instrumentation;

/**
 * An instrumented event. Use in conjuction with an EventRecorder to log and/or aggregate events.
 * Events have a Name and Id: the Name provides the unit for aggregation, while the Id differentiates
 * events within the same aggregate.
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
   * The Name, which is the unit for aggregation
   * @return Name
   */
  public String getName();

  /**
   * The Id, which differentiates events within the same aggregation Name
   * @return Id
   */
  public String getId();

  /**
   * A JSON representation of the Event. Should be used when logging the Event.
   * @return A Json String
   */
  public String toJsonString();
}
