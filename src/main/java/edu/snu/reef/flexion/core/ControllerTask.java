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

    userControllerTask.run();
    while (!userControllerTask.isTerminate()){
      ctrlMessageBroadcast.send(CtrlMessage.RUN);
      sendData();
      receiveData();
      userControllerTask.run();
    }
    ctrlMessageBroadcast.send(CtrlMessage.TERMINATE);

    return null;
  }

  private void sendData() throws Exception {

      if(userControllerTask.isBroadcastUsed()) {
          commGroup.getBroadcastSender(DataBroadcast.class).send(
                  ((IDataBroadcastSender) userControllerTask).sendBroadcastData());
      }

      if(userControllerTask.isScatterUsed()) {
          commGroup.getScatterSender(DataScatter.class).send(
                  ((IDataScatterSender) userControllerTask).sendScatterData());
      }
  }

  private void receiveData() throws Exception {

      if(userControllerTask.isGatherUsed()) {
          ((IDataGatherReceiver)userControllerTask).receiveGatherData(
                  commGroup.getBroadcastReceiver(DataGather.class).receive());
      }

      if(userControllerTask.isReduceUsed()) {
          ((IDataReduceReceiver)userControllerTask).receiveReduceData(
                  commGroup.getReduceReceiver(DataReduce.class).reduce());
      }

  }

}
