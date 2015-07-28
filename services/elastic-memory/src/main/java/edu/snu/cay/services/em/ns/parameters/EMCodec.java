package edu.snu.cay.services.em.ns.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.remote.Codec;

@NamedParameter(doc = "Network connection service codec for EM")
public final class EMCodec implements Name<Codec<?>> {
}
