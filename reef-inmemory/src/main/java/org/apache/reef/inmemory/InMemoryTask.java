package org.apache.reef.inmemory;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.TaskMessage;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.wake.EStage;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.StageConfiguration;
import com.microsoft.wake.impl.ThreadPoolStage;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.cache.BlockId;
import org.apache.reef.inmemory.cache.BlockLoader;
import org.apache.reef.inmemory.cache.InMemoryCache;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockLoader;
import org.apache.reef.inmemory.cache.hdfs.HdfsBlockMessage;
import org.apache.reef.inmemory.cache.hdfs.HdfsMessage;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
@Unit
public class InMemoryTask implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final ObjectSerializableCodec<HdfsMessage> HDFS_CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private final int numThreads;
  private transient Optional<TaskMessage> hbMessage = Optional.empty();

  private final InMemoryCache cache;

  private boolean isDone = false;
  private EStage<BlockLoader> loadingStage;


  @Inject
  InMemoryTask(final InMemoryCache cache,
               final int numThreads) throws InjectionException {
    this.cache = cache;
    this.numThreads = numThreads;
    this.hbMessage.orElse(INIT_MESSAGE).get();
    this.loadingStage = initStage();
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
    final byte[] report = cache.getReport();
    InMemoryTask.this.hbMessage = Optional.of(TaskMessage.from(this.toString(), report));
    return this.hbMessage;
  }

  public final class DriverMessageHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(DriverMessage driverMessage) {
      if (driverMessage.get().isPresent()) {
        HdfsMessage msg = HDFS_CODEC.decode(driverMessage.get().get());
        if (msg.getBlockMessage().isPresent()) {
          LOG.log(Level.INFO, "Received load block msg");
          HdfsBlockMessage blockMsg = msg.getBlockMessage().get();
          HdfsBlockLoader loader = new HdfsBlockLoader(blockMsg.getBlockId(), blockMsg.getLocations().get(0));
          executeLoad(loader);
        } else if (msg.getClearMessage().isPresent()) {
          LOG.log(Level.INFO, "Received cache clear msg");
          cache.clear();
        }
      }

    }
  }

  private static class LoadExecutor implements EventHandler<BlockLoader> {
    private final InMemoryCache cache;
    @Inject
    LoadExecutor(final InMemoryCache cache) {
      this.cache = cache;
    }
    @Override
    public void onNext(BlockLoader loader) {
      try {
        LOG.log(Level.INFO, "Start loading block");
        BlockId blockId = loader.getBlockId();
        ByteBuffer result = loader.loadBlock();
        cache.put(blockId, result);
        LOG.log(Level.INFO, "Finish loading block");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private EStage<BlockLoader> initStage() throws InjectionException {
    JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindImplementation(BlockLoader.class, HdfsBlockLoader.class);

    LoadExecutor executor;
    {
      Injector i = Tang.Factory.getTang().newInjector(cb.build());
      i.bindVolatileInstance(InMemoryCache.class, InMemoryTask.this.cache);
      executor = i.getInstance(LoadExecutor.class);
    }

    cb.bindImplementation(EStage.class, ThreadPoolStage.class);
    cb.bindNamedParameter(StageConfiguration.NumberOfThreads.class, String.valueOf(numThreads));
    Injector i = Tang.Factory.getTang().newInjector(cb.build());
    i.bindVolatileParameter(StageConfiguration.StageHandler.class, executor);
    return i.getInstance(EStage.class);
  }

  public void executeLoad(BlockLoader loader) {
    loadingStage.onNext(loader);
  }
}