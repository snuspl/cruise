package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.group.operators.Broadcast;
import com.microsoft.reef.io.network.group.operators.Reduce;
import com.microsoft.reef.io.network.nggroup.api.task.CommunicationGroupClient;
import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import edu.snu.reef.flexion.groupcomm.names.CommunicationGroup;
import edu.snu.reef.flexion.groupcomm.names.CtrlMsgBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataReduce;

import javax.inject.Inject;

public final class FlexionServiceCtrl implements FlexionCommunicator {
  private final CommunicationGroupClient commGroup;
  private final Broadcast.Sender<CtrlMessage> ctrlMsgBroadcast;
  private final Broadcast.Sender<Integer> dataBroadcast;
  private final Reduce.Receiver<Integer> dataReduce;


  FlexionServiceCtrl(final GroupCommClient groupCommClient) {
    this.commGroup = groupCommClient.getCommunicationGroup(CommunicationGroup.class);
    this.ctrlMsgBroadcast = commGroup.getBroadcastSender(CtrlMsgBroadcast.class);
    this.dataBroadcast = commGroup.getBroadcastSender(DataBroadcast.class);
    this.dataReduce = commGroup.getReduceReceiver(DataReduce.class);
  }

  @Override
  public final void send(final Integer dataToSend) throws Exception {
    ctrlMsgBroadcast.send(CtrlMessage.RUN);
    dataBroadcast.send(dataToSend);
  }

  @Override
  public final Integer receive() throws Exception {
    return dataReduce.reduce();
  }

  @Override
  public final boolean terminate() throws Exception {
    ctrlMsgBroadcast.send(CtrlMessage.TERMINATE);
    return true;
  }
}
