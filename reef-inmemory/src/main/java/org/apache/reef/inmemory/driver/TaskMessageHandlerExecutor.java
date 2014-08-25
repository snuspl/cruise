package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.task.TaskMessage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.driver.service.SurfMetaServer;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

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
