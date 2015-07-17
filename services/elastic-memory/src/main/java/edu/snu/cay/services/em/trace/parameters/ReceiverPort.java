package edu.snu.cay.services.em.trace.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Receiver Port", default_value = "9410", short_name = "htrace_receiver_port")
public final class ReceiverPort implements Name<Integer> {
}
