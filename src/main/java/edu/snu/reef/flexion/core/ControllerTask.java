package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataScatterSender;
import edu.snu.reef.flexion.groupcomm.names.*;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ControllerTask implements Task {
    private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());
    public final static String TASK_ID = "CtrlTask";

    private final UserControllerTask userControllerTask;
    private final CommunicationGroupClient commGroup;

    private final Broadcast.Sender<CtrlMessage> ctrlMessageBroadcast;

    @Inject
    public ControllerTask(final GroupCommClient groupCommClient,
                          final UserControllerTask userControllerTask) {
        this.commGroup = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
        this.userControllerTask = userControllerTask;
        this.ctrlMessageBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
    }

    @Override
    public final byte[] call(final byte[] memento) throws Exception {
        LOG.log(Level.INFO, "CtrlTask commencing...");

        int iteration = 0;
        while (!userControllerTask.isTerminated(iteration)) {
            userControllerTask.run(iteration);
            ctrlMessageBroadcast.send(CtrlMessage.RUN);
            sendData(iteration);
            receiveData();
            topologyChanged();
            iteration++;
        }
        ctrlMessageBroadcast.send(CtrlMessage.TERMINATE);

        return null;
    }

    /**
     * Check if group communication topology has changed, and updates it if it has.
     * @return true if topology has changed, false if not
     */
    private final boolean topologyChanged() {
        if (commGroup.getTopologyChanges().exist()) {
            commGroup.updateTopology();
            return true;

        } else {
            return false;
        }
    }


    private void sendData(int iteration) throws Exception {

        if (userControllerTask.isBroadcastUsed()) {
            commGroup.getBroadcastSender(DataBroadcast.class).send(
                    ((IDataBroadcastSender) userControllerTask).sendBroadcastData(iteration));
        }

        if (userControllerTask.isScatterUsed()) {
            commGroup.getScatterSender(DataScatter.class).send(
                    ((IDataScatterSender) userControllerTask).sendScatterData(iteration));
        }
    }

    private void receiveData() throws Exception {

        if (userControllerTask.isGatherUsed()) {
            ((IDataGatherReceiver)userControllerTask).receiveGatherData(
                    commGroup.getGatherReceiver(DataGather.class).receive());
        }

        if (userControllerTask.isReduceUsed()) {

            ((IDataReduceReceiver)userControllerTask).receiveReduceData(
                    commGroup.getReduceReceiver(DataReduce.class).reduce());

        }

    }

}
