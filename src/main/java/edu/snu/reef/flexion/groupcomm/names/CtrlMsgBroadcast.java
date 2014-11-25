package edu.snu.reef.flexion.groupcomm.names;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name for the operation used to manage control messages")
public final class CtrlMsgBroadcast implements Name<String> {
}
