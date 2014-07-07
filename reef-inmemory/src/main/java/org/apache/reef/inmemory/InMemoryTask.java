package org.apache.reef.inmemory;

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
import org.apache.reef.inmemory.cache.BlockId;
import org.apache.reef.inmemory.cache.BlockLoader;
import org.apache.reef.inmemory.cache.CacheStatusMessage;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockLoader;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;
import org.apache.reef.inmemory.cache.service.SurfCacheServer;

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
  private static final ObjectSerializableCodec<HdfsMessage> HDFS_CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient Optional<TaskMessage> hbMessage = Optional.empty();

  private final InMemoryCache cache;

  private ExecutorService executor;
  private final SurfCacheServer dataServer;

  private boolean isDone = false;
  private EStage<BlockLoader> loadingStage;

  @Inject
  InMemoryTask(final InMemoryCache cache,
               final SurfCacheServer dataServer,
               final EStage<BlockLoader> loadingStage) throws InjectionException {
    this.cache = cache;
    this.dataServer = dataServer;
    this.hbMessage.orElse(INIT_MESSAGE).get(); // TODO: Is this necessary?
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
        final HdfsMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
        if (msg.getBlockMessage().isPresent()) {
          LOG.log(Level.INFO, "Received load block msg");
          final HdfsBlockMessage blockMsg = msg.getBlockMessage().get();
          try {
            final HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getLocations().get(0));
            executeLoad(loader);
          } catch (IOException e ) {
            LOG.log(Level.SEVERE, "Exception occured while loading");
          }
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
      try {
        final BlockId blockId = loader.getBlockId();
        LOG.log(Level.INFO, "Add stub block");
        cache.putPending(blockId);
        LOG.log(Level.INFO, "Start loading block");
        final byte[] result = loader.loadBlock();
        cache.put(blockId, result);
        LOG.log(Level.INFO, "Finish loading block");
      } catch (IOException e) {
        e.printStackTrace();
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