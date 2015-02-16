package edu.snu.reef.flexion.examples;

import edu.snu.reef.flexion.core.UserControllerTask;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;

import javax.inject.Inject;

public final class SimpleCtrlTask extends UserControllerTask
        implements IDataReduceReceiver<Integer>, IDataBroadcastSender<Integer> {

    private Integer iteration = 0;
    private Integer receivedData = 0;
    private Integer dataToSend = 0;

    @Inject
    private SimpleCtrlTask() {
    }


    @Override
    public void run() {
        dataToSend = receivedData * 2;
    }

    @Override
    public boolean isTerminate() {
        return iteration++ < 10;
    }

    @Override
    public Integer sendBroadcastData() {
        return dataToSend;
    }

    @Override
    public void receiveReduceData(Integer data) {
        receivedData = data;
    }
}
