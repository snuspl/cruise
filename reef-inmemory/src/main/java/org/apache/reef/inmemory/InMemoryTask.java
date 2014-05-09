package org.apache.reef.inmemory;

import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.inmemory.cache.CacheImpl;

import com.microsoft.reef.task.Task;
import com.microsoft.reef.task.TaskMessage;
import com.microsoft.reef.task.TaskMessageSource;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
public class InMemoryTask implements Task, TaskMessageSource {

  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  private static final TaskMessage INIT_MESSAGE = TaskMessage.from("", CODEC.encode("MESSAGE::INIT"));
  private transient Optional<TaskMessage> hbMessage = Optional.empty();
  CacheImpl cache;
  
  private boolean isDone = false;

  @Inject
  InMemoryTask() {
    this.cache = new CacheImpl();
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
    byte[] report = cache.getReport();
    InMemoryTask.this.hbMessage = Optional.of(TaskMessage.from(this.toString(), report));
    return this.hbMessage;
  }
}