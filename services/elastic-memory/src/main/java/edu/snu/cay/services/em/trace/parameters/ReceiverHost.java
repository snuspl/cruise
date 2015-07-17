package edu.snu.cay.services.em.trace.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Receiver Host", default_value = "localhost", short_name = "htrace_receiver_host")
public final class ReceiverHost implements Name<String> {
}
