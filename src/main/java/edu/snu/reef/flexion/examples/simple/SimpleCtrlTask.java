package edu.snu.reef.flexion.examples.simple;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;

import javax.inject.Inject;

public final class SimpleCtrlTask extends UserControllerTask
    implements DataReduceReceiver<Integer>, DataBroadcastSender<String> {

  private Integer count = 0;

  @Inject
  private SimpleCtrlTask() {
  }


  @Override
  public void run(int iteration) {
    System.out.println(String.format("Number of Tasks Printing Message: %d", count));
  }

  @Override
  public boolean isTerminated(int iteration) {
    return iteration >= 10;
  }

  @Override
  public String sendBroadcastData(int iteration) {
    return String.format("Hello, REEF!: %d", iteration);
  }

  @Override
  public void receiveReduceData(Integer data) {
    this.count = data;
  }
}
