package org.apache.reef.inmemory.task;

import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.wake.EventHandler;

public interface DriverMessageHandler extends EventHandler<DriverMessage> {
}
