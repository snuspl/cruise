package org.apache.reef.inmemory;

import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.inmemory.cache.CacheImpl;

import com.microsoft.reef.task.Task;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;

/**
 * InMemory Task. Wait until receiving a signal from Driver.
 */
public class InMemoryTask implements Task {
  private static final Logger LOG = Logger.getLogger(InMemoryTask.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();
  
  private boolean isDone = false;
  CacheImpl cache = null;

  @Inject
  InMemoryTask() {
    this.cache = new CacheImpl();
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
}