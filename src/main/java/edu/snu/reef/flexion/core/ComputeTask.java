package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataGatherSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataScatterReceiver;
import edu.snu.reef.flexion.groupcomm.names.*;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ComputeTask implements Task, TaskMessageSource {
    private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());
    public final static String TASK_ID = "CmpTask";

    private final UserComputeTask userComputeTask;
    private final CommunicationGroupClient commGroup;
    private final HeartBeatTriggerManager heartBeatTriggerManager;

    private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;

    private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();

    private final DataParser dataParser;

    private long runTime = -1;

    @Inject
    public ComputeTask(final GroupCommClient groupCommClient,
                       final DataParser dataParser,
                       final UserComputeTask userComputeTask,
                       final HeartBeatTriggerManager heartBeatTriggerManager) {

        this.userComputeTask = userComputeTask;
        this.dataParser = dataParser;
        this.commGroup = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
        this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
        this.heartBeatTriggerManager = heartBeatTriggerManager;
    }

    @Override
    public final byte[] call(final byte[] memento) throws Exception {
        LOG.log(Level.INFO, "CmpTask commencing...");

        while (!isTerminated()) {
            receiveData();
            final long runStart = System.currentTimeMillis();
            userComputeTask.run(dataParser.get());
            runTime = System.currentTimeMillis() - runStart;
            sendData();
            heartBeatTriggerManager.triggerHeartBeat();
        }

        return null;
    }


    private void receiveData() throws Exception {

        if (userComputeTask.isBroadcastUsed()) {
            ((IDataBroadcastReceiver)userComputeTask).receiveBroadcastData(
                    commGroup.getBroadcastReceiver(DataBroadcast.class).receive());

        }

        if (userComputeTask.isScatterUsed()) {
            ((IDataScatterReceiver)userComputeTask).receiveScatterData(
                    commGroup.getScatterReceiver(DataScatter.class).receive());
        }

    };

    private void sendData() throws Exception {

        if (userComputeTask.isGatherUsed()) {
            commGroup.getGatherSender(DataGather.class).send(
                    ((IDataGatherSender)userComputeTask).sendGatherData());
        }

        if (userComputeTask.isReduceUsed()) {
            commGroup.getReduceSender(DataReduce.class).send(
                    ((IDataReduceSender)userComputeTask).sendReduceData());
        }
    }

    private boolean isTerminated() throws Exception{

        return ctrlMessageBroadcast.receive() == CtrlMessage.TERMINATE;
    }

    @Override
    public synchronized Optional<TaskMessage> getMessage() {
        return Optional.of(TaskMessage.from(ComputeTask.class.getName(),
                this.codecLong.encode(this.runTime)));
    }
}
