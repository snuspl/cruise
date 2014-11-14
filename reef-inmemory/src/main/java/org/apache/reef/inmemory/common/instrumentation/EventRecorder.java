package org.apache.reef.inmemory.common.instrumentation;

/**
 * The Recorder of Events. You can create/start/stop/record in two lines, using the following idiom
 * (first instatiate the Recorder instance as RECORD via Tang):
 *
 *   final Event event = RECORD.event(group, id).start();
 *   ( ... code to time ... )
 *   RECORD.record(event.stop());
 *
 * Events have a Group and Id: the Group provides the unit for aggregation, while the Id differentiates
 * events within the same aggregate.
 *
 * Users should make a best effort to provide a unique Id across the same Group. This will help
 * make sense of the recorded events, e.g., during post-processing of logs. Similarly, users should
 * make a best effort to keep the number of Groups manageable, to avoid polluting aggregate statistics, e.g.,
 * by creating too many entries to show on a Ganglia screen.
 *
 * Users should make sure start or stop should always be called on an event at most once.
 * Other than this assumption, implementations must guarantee that Event and EventRecorder methods are thread-safe.
 */
public interface EventRecorder {
  /**
   * Create an Event with the given Group and Id
   * @param group The Group, which is the unit for aggregation
   * @param id The Id, which differentiates events within the same aggregation Name
   * @return The created event
   */
  Event event(String group, String id);

  /**
   * Record the Event.
   * @param event Event to record
   */
  void record(Event event);
}
