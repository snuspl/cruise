package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;

import javax.inject.Inject;

public final class SimpleCtrlTask extends UserControllerTask
        implements IDataReduceReceiver<Integer>, IDataBroadcastSender<Integer> {

    private Integer receivedData = 0;
    private Integer dataToSend = 0;

    @Inject
    private SimpleCtrlTask() {
    }


    @Override
    public void run(int iteration) {
        dataToSend = receivedData * 2;
    }

    @Override
    public boolean isTerminated(int iteration) {
        return iteration > 10;
    }

    @Override
    public Integer sendBroadcastData(int iteration) {
        return dataToSend;
    }

    @Override
    public void receiveReduceData(Integer data) {
        receivedData = data;
    }
}
