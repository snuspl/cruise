package org.apache.reef.inmemory;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.TaskMessage;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.task.events.DriverMessage;
import com.microsoft.reef.util.Optional;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import org.apache.reef.inmemory.cache.InMemoryCache;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
@Unit
public class InMemoryTask implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient Optional<TaskMessage> hbMessage = Optional.empty();

  private final InMemoryCache cache;
  private final org.apache.reef.inmemory.cache.DriverMessageHandler driverMessageHandler;

  private boolean isDone = false;

  @Inject
  InMemoryTask(InMemoryCache cache,
               org.apache.reef.inmemory.cache.DriverMessageHandler driverMessageHandler) {
    this.cache = cache;
    this.driverMessageHandler = driverMessageHandler;
    this.hbMessage.orElse(INIT_MESSAGE).get();
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
      driverMessageHandler.onNext(driverMessage);
    }
  }
}