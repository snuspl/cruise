package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataScatterReceiver;
import edu.snu.reef.flexion.groupcomm.names.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ComputeTask implements Task, TaskMessageSource {
    private final static Logger LOG = Logger.getLogger(ComputeTask.class.getName());
    public final static String TASK_ID = "CmpTask";

    private final UserComputeTask userComputeTask;
    private final CommunicationGroupClient commGroup;
    private final KeyValueStore keyValueStore;
    private final HeartBeatTriggerManager heartBeatTriggerManager;

    private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;

    private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();

    private final DataParser dataParser;

    private long runTime = -1;

    @Inject
    public ComputeTask(final GroupCommClient groupCommClient,
                       final DataParser dataParser,
                       final KeyValueStore keyValueStore,
                       final UserComputeTask userComputeTask,
                       @Parameter(CommunicationGroup.class) final String commGroupName,
                       final HeartBeatTriggerManager heartBeatTriggerManager) throws ClassNotFoundException {

        this.userComputeTask = userComputeTask;
        this.dataParser = dataParser;
        this.keyValueStore = keyValueStore;
        this.commGroup = groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
        this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
        this.heartBeatTriggerManager = heartBeatTriggerManager;
    }

    @Override
    public final byte[] call(final byte[] memento) throws Exception {
        LOG.log(Level.INFO, "CmpTask commencing...");

        Object dataSet = dataParser.get();
        userComputeTask.initialize(keyValueStore);
        int iteration=0;
        while (!isTerminated()) {
            receiveData();
            final long runStart = System.currentTimeMillis();
            userComputeTask.run(iteration, dataSet);
            runTime = System.currentTimeMillis() - runStart;
            sendData(iteration);
            heartBeatTriggerManager.triggerHeartBeat();
            iteration++;
        }
        userComputeTask.cleanup(keyValueStore);

        return null;
    }


    private void receiveData() throws Exception {

        if (userComputeTask.isBroadcastUsed()) {
            ((DataBroadcastReceiver)userComputeTask).receiveBroadcastData(
                    commGroup.getBroadcastReceiver(DataBroadcast.class).receive());

        }

        if (userComputeTask.isScatterUsed()) {
            ((DataScatterReceiver)userComputeTask).receiveScatterData(
                    commGroup.getScatterReceiver(DataScatter.class).receive());
        }

    };

    private void sendData(int iteration) throws Exception {

        if (userComputeTask.isGatherUsed()) {
            commGroup.getGatherSender(DataGather.class).send(
                    ((DataGatherSender)userComputeTask).sendGatherData(iteration));
        }

        if (userComputeTask.isReduceUsed()) {
            commGroup.getReduceSender(DataReduce.class).send(
                    ((DataReduceSender)userComputeTask).sendReduceData(iteration));
        }
    }

    private boolean isTerminated() throws Exception {

        return ctrlMessageBroadcast.receive() == CtrlMessage.TERMINATE;
    }

    @Override
    public synchronized Optional<TaskMessage> getMessage() {
        return Optional.of(TaskMessage.from(ComputeTask.class.getName(),
                this.codecLong.encode(this.runTime)));
    }
}
