package org.apache.reef.inmemory.common.instrumentation;

/**
 * The Recorder of Events. You can create/start/stop/record in two lines, using the following idiom
 * (first instatiate the Recorder instance as RECORD via Tang):
 *
 *   final Event event = RECORD.event(name, id).start();
 *   ( ... code to time ... )
 *   RECORD.record(event.stop());
 */
public interface EventRecorder {
  /**
   * Create an Event with the given Name and Id
   * @param name The Name, which is the unit for aggregation
   * @param id The Id, which differentiates events within the same aggregation Name
   * @return The created event
   */
  Event event(String name, String id);

  /**
   * Record the Event
   * @param event Event to record
   */
  void record(Event event);
}
