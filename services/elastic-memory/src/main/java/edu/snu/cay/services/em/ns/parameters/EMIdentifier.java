package edu.snu.cay.services.em.ns.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.remote.Codec;

@NamedParameter(doc = "Network connection factory identifier for EM", default_value = "EM")
public final class EMIdentifier implements Name<String> {
}
