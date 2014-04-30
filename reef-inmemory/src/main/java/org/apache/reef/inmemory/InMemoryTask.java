package org.apache.reef.inmemory;

import javax.inject.Inject;

import com.microsoft.reef.task.Task;

/**
 * InMemory Task. Print a message.
 */
public class InMemoryTask implements Task {

  @Inject
  InMemoryTask() {
  }

  @Override
  public byte[] call(byte[] arg0) throws Exception {
    System.out.println("InMemoryTaskCalled");
    return null;
  }

}