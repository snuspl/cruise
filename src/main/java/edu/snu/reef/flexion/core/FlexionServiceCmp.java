package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.names.CommunicationGroup;
import edu.snu.reef.flexion.groupcomm.names.CtrlMsgBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataReduce;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.logging.Level;

public final class FlexionServiceCmp implements FlexionCommunicator {
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Receiver<CtrlMessage> ctrlMessageBroadcast;
  private final Broadcast.Receiver<Integer> dataBroadcast;
  private final Reduce.Sender<Integer> dataReduce;

  private final HeartBeatTriggerManager heartBeatTriggerManager;

  FlexionServiceCmp(final GroupCommClient groupCommClient,
                    final HeartBeatTriggerManager heartBeatTriggerManager) {
    this.commGroup = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
    this.ctrlMessageBroadcast = commGroup.getBroadcastReceiver(CtrlMsgBroadcast.class);
    this.dataBroadcast = commGroup.getBroadcastReceiver(DataBroadcast.class);
    this.dataReduce = commGroup.getReduceSender(DataReduce.class);

    this.heartBeatTriggerManager = heartBeatTriggerManager;
  }

  @Override
  public final void send(final Integer dataToSend) throws Exception {
    dataReduce.send(dataToSend);
    heartBeatTriggerManager.triggerHeartBeat();
  }

  @Override
  public final Integer receive() throws Exception {
    return dataBroadcast.receive();
  }

  @Override
  public final boolean terminate() throws Exception {
    return ctrlMessageBroadcast.receive() == CtrlMessage.TERMINATE;
  }
}
