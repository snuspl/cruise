package org.apache.reef.inmemory.task;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.TaskMessage;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.reef.task.events.TaskStart;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.common.hdfs.HdfsDriverTaskMessage;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;
import org.apache.reef.inmemory.task.hdfs.HdfsBlockLoader;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.task.hdfs.TokenDecodeFailedException;
import org.apache.reef.inmemory.task.service.SurfCacheServer;

import javax.inject.Inject;
import java.io.IOException;
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

  private boolean isDone = false;

  @Inject
  InMemoryTask(final InMemoryCache cache,
               final SurfCacheServer dataServer,
               final DriverMessageHandler driverMessageHandler) throws InjectionException {
    this.cache = cache;
    this.dataServer = dataServer;
    this.driverMessageHandler = driverMessageHandler;
  }

  /**
   * Wait until receiving a signal.
   * TODO notify this and set isDone to be true to wake up
   */
  @Override
  public byte[] call(byte[] arg0) throws Exception {
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
    final CacheStatusMessage message = new CacheStatusMessage(cache.getStatistics(), dataServer.getBindPort());
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
