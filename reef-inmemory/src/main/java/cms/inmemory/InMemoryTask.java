package cms.inmemory;

import javax.inject.Inject;

import com.microsoft.reef.task.Task;

public class InMemoryTask implements Task {

  @Inject
  InMemoryTask() {
  }

  @Override
  public byte[] call(byte[] arg0) throws Exception {
    System.out.println("Hello");
    return null;
  }

}