package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;

import javax.inject.Inject;
import java.util.List;

public final class SimpleCmpTask extends UserComputeTask <List<String>>
        implements DataBroadcastReceiver<Integer>, DataReduceSender<Integer> {

    private Integer receivedData = 0;
    private Integer dataToSend = 0;

    @Inject
    private SimpleCmpTask() {
    }

    @Override
    public void run(int iter, List<String> data) {
        float increment = 0;
        for (int i = 0; i < 500000; i++) {
            increment += Math.random();
        }
        dataToSend = (int) (receivedData + increment);
    }

    @Override
    public void receiveBroadcastData(Integer data) {
        receivedData = data;
    }

    @Override
    public Integer sendReduceData(int iteration) {
        return dataToSend;
    }

}



