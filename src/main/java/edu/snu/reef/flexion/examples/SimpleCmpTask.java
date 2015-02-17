package edu.snu.reef.flexion.examples;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceSender;
import edu.snu.reef.flexion.groupcomm.subs.DataReduceFunction;

import javax.inject.Inject;

public final class SimpleCmpTask extends UserComputeTask
        implements IDataBroadcastReceiver<Integer>, IDataReduceSender<Integer> {

    private Integer receivedData = 0;
    private Integer dataToSend = 0;

    @Inject
    private SimpleCmpTask() {
    }

    @Override
    public void run() {
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
    public Integer sendReduceData() {
        return dataToSend;
    }

    @Override
    public Class <? extends Reduce.ReduceFunction<Integer>> getReduceFunction() {
        return DataReduceFunction.class;
    }

}


