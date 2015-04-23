package edu.snu.reef.flexion.examples.simple;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;

import javax.inject.Inject;

public final class SimpleCmpTask extends UserComputeTask
    implements DataBroadcastReceiver<String>, DataReduceSender<Integer> {

  private String message = null;
  private Integer count = 0;

  @Inject
  private SimpleCmpTask() {
  }

  @Override
  public void run(int iteration) {
    System.out.println(message);
    count++;

  }

  @Override
  public void receiveBroadcastData(String data) {
    message = data;
    count = 0;
  }

  @Override
  public Integer sendReduceData(int iteration) {
    return count;
  }

}



