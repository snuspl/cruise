package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastSender;
import edu.snu.reef.flexion.groupcomm.interfaces.DataGatherReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataScatterSender;
import edu.snu.reef.flexion.groupcomm.names.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ControllerTask implements Task {
    private final static Logger LOG = Logger.getLogger(ControllerTask.class.getName());
    public final static String TASK_ID = "CtrlTask";

    private final UserControllerTask userControllerTask;
    private final CommunicationGroupClient commGroup;
    private final KeyValueStore keyValueStore;

    private final Broadcast.Sender<CtrlMessage> ctrlMessageBroadcast;

    @Inject
    public ControllerTask(final GroupCommClient groupCommClient,
                          final KeyValueStore keyValueStore,
                          final UserControllerTask userControllerTask,
                          @Parameter(CommunicationGroup.class) final String commGroupName) throws ClassNotFoundException {

        this.commGroup = groupCommClient.getCommunicationGroup((Class<? extends Name<String>>) Class.forName(commGroupName));
        this.keyValueStore = keyValueStore;
        this.userControllerTask = userControllerTask;
        this.ctrlMessageBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
    }

    @Override
    public final byte[] call(final byte[] memento) throws Exception {
        LOG.log(Level.INFO, "CtrlTask commencing...");

        int iteration = 0;
        userControllerTask.initialize(keyValueStore);
        do {
            userControllerTask.run(iteration);
            ctrlMessageBroadcast.send(CtrlMessage.RUN);
            sendData(iteration);
            receiveData();
            topologyChanged();
        } while(!userControllerTask.isTerminated(iteration++));
        userControllerTask.cleanup(keyValueStore);
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
                    ((DataBroadcastSender) userControllerTask).sendBroadcastData(iteration));
        }

        if (userControllerTask.isScatterUsed()) {
            commGroup.getScatterSender(DataScatter.class).send(
                    ((DataScatterSender) userControllerTask).sendScatterData(iteration));
        }
    }

    private void receiveData() throws Exception {

        if (userControllerTask.isGatherUsed()) {
            ((DataGatherReceiver)userControllerTask).receiveGatherData(
                    commGroup.getGatherReceiver(DataGather.class).receive());
        }

        if (userControllerTask.isReduceUsed()) {

            ((DataReduceReceiver)userControllerTask).receiveReduceData(
                    commGroup.getReduceReceiver(DataReduce.class).reduce());

        }

    }

}
