package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.task.service.SurfCacheServer;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.task.events.TaskStart;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
@Unit
public class InMemoryTask implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final ObjectSerializableCodec<CacheStatusMessage> STATUS_CODEC = new ObjectSerializableCodec<>();

  private ExecutorService executor; // TODO shouldn't we shutdown this executor when the Task is finished?
  private final SurfCacheServer dataServer;
  private final InMemoryCache cache;
  private final DriverMessageHandler driverMessageHandler;
  private final HeartBeatTriggerManager heartBeatTriggerManager;

  private boolean isDone = false;

  @Inject
  InMemoryTask(final InMemoryCache cache,
               final SurfCacheServer dataServer,
               final DriverMessageHandler driverMessageHandler,
               final HeartBeatTriggerManager heartBeatTriggerManager) throws InjectionException {
    this.cache = cache;
    this.dataServer = dataServer;
    this.driverMessageHandler = driverMessageHandler;
    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  /**
   * Wait until receiving a signal.
   * TODO notify this and set isDone to be true to wake up
   */
  @Override
  public byte[] call(byte[] arg0) throws Exception {
    this.heartBeatTriggerManager.triggerHeartBeat();
    final String message = "Done";
    while(true) {
      synchronized (this) {
        this.wait();
        if(this.isDone)
          break;
      }
    }
    return CODEC.encode(message);
  }

  @Override
  public Optional<TaskMessage> getMessage() {
    final CacheStatusMessage message = new CacheStatusMessage(cache.getStatistics(), cache.pullUpdates(), dataServer.getBindPort());
    return Optional.of(TaskMessage.from(this.toString(),
      STATUS_CODEC.encode(message)));
  }

  /**
   * Starts the thread for fulfilling data requests from clients
   */
  public final class StartHandler implements EventHandler<TaskStart> {
    @Override
    public void onNext(final TaskStart taskStart) {
      LOG.log(Level.INFO, "TaskStart: {0}", taskStart);
      executor = Executors.newSingleThreadExecutor();
      try {
        final int bindPort = dataServer.initBindPort();
        LOG.log(Level.INFO, "Cache bound to port: {0}", bindPort);
        executor.execute(dataServer);
      } catch (Exception ex) {
        final String message = "Failed to start Surf Meta Service";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message, ex);
      }
    }
  }

  /**
   * Delegates to the DriverMessageHandler for the Base FS
   */
  public final class DelegatingDriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(DriverMessage driverMessage) {
      driverMessageHandler.onNext(driverMessage);
    }
  }
}
