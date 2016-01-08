package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.driver.service.SurfMetaServer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for the update handling stage. The update is handled
 * with a thread in the stage pool. Updates from the same cache
 * may be dispatched concurrently (although not likely with a large enough heartbeat).
 * If concurrent processing is not desired,
 * downstream methods must implement synchronization to prevent it.
 */
public final class TaskMessageHandlerExecutor implements EventHandler<TaskMessage> {

  private static final Logger LOG = Logger.getLogger(TaskMessageHandlerExecutor.class.getName());
  private static final ObjectSerializableCodec<CacheStatusMessage> CODEC = new ObjectSerializableCodec<>();

  private final SurfMetaServer surfMetaServer;

  @Inject
  public TaskMessageHandlerExecutor(final SurfMetaServer surfMetaServer) {
    this.surfMetaServer = surfMetaServer;
  }

  @Override
  public void onNext(TaskMessage msg) {
    LOG.log(Level.FINE, "TaskMessage: from {0}: {1}",
            new Object[]{msg.getId(), CODEC.decode(msg.get())});

    surfMetaServer.handleUpdate(msg.getId(), CODEC.decode(msg.get()));
  }
}
