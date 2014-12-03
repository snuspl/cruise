package org.apache.reef.inmemory.task;

import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.wake.EventHandler;

/**
 * Handles messages from the Driver, related to cache loading and management.
 * Each base FS should implement the handler and decode it using the FS-dependent message codec.
 */
public interface DriverMessageHandler extends EventHandler<DriverMessage> {
}
