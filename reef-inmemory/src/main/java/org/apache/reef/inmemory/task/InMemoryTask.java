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
import org.apache.reef.inmemory.common.CacheMessage;
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
  private static final ObjectSerializableCodec<CacheMessage> HDFS_CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));

  private ExecutorService executor; // TODO shouldn't we shutdown this executor when the Task is finished?
  private final SurfCacheServer dataServer;
  private final InMemoryCache cache;

  private boolean isDone = false;
  private EStage<BlockLoader> loadingStage;

  @Inject
  InMemoryTask(final InMemoryCache cache,
               final SurfCacheServer dataServer,
               final EStage<BlockLoader> loadingStage) throws InjectionException {
    this.cache = cache;
    this.dataServer = dataServer;
    this.loadingStage = loadingStage;
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
    final CacheStatusMessage message = new CacheStatusMessage(dataServer.getBindPort());
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
        LOG.log(Level.INFO, "Cache bound to port: {0}"+bindPort);
        executor.execute(dataServer);
      } catch (Exception ex) {
        final String message = "Failed to start Surf Meta Service";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message, ex);
      }
    }
  }

  /**
   * Handles messages from the Driver. The message contains the information
   * what this task is supposed to do.
   * TODO Separate Hdfs-specific part
   */
  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(DriverMessage driverMessage) {
      if (driverMessage.get().isPresent()) {
        final CacheMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
        if (msg.getHdfsBlockMessage().isPresent()) {
          LOG.log(Level.INFO, "Received load block msg");
          final HdfsBlockMessage blockMsg = msg.getHdfsBlockMessage().get();

          // TODO: pass request to InMemoryCache. IMC can check if block already exists, call executeLoad if not.
          // TODO: loader should receive all block locations
          final HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getLocations());
          executeLoad(loader);

        } else if (msg.getClearMessage().isPresent()) {
          LOG.log(Level.INFO, "Received cache clear msg");
          cache.clear();
        }
      }

    }
  }

  /**
   * Handler for the loading stage. This executes block loading with
   * a thread allocated from the thread pool of the loading stage.
   */
  public static class LoadExecutor implements EventHandler<BlockLoader> {
    private final InMemoryCache cache;
    @Inject
    LoadExecutor(final InMemoryCache cache) {
      this.cache = cache;
    }
    @Override
    public void onNext(BlockLoader loader) {
      // TODO would it be better to report to driver?
      // It seems possible either to send an message directly or
      // to keep the failure info and send via Heartbeat
      try {
        cache.load(loader);
        LOG.log(Level.INFO, "Finish loading block");
      } catch (ConnectionFailedException e) {
        LOG.log(Level.SEVERE, "Failed to load block {0} because of connection failure", loader.getBlockId());
      } catch (TokenDecodeFailedException e) {
        LOG.log(Level.SEVERE, "Failed to load block {0}, HdfsToken is not valid", loader.getBlockId());
      } catch (TransferFailedException e) {
        LOG.log(Level.SEVERE, "An error occurred while transferring the block {0} from the Datanode", loader.getBlockId());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Unhandled Exception :", e.getCause());
      }
    }
  }

  /**
   * Submit an event to the loading stage
   */
  public void executeLoad(BlockLoader loader) {
    loadingStage.onNext(loader);
  }
}