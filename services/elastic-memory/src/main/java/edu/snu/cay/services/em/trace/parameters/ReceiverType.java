package edu.snu.cay.services.em.trace.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "ReceiverType = ZIPKIN|STDOUT", default_value = "STDOUT", short_name = "htrace_receiver_type")
public final class ReceiverType implements Name<String> {
}
