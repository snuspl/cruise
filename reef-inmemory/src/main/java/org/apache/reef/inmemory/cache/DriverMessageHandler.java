package org.apache.reef.inmemory.cache;

import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.EventHandler;

/**
 * Interface for handling DriverMessages. A new implementation must be provided
 * for each underlying filesystem, which supports the FS's message formats.
 */
public interface DriverMessageHandler extends EventHandler<DriverMessage> {
}
