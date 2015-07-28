package edu.snu.cay.services.em.ns.parameters;

import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;

@NamedParameter(doc = "Network message receive handler for EM")
public final class EMMessageHandler implements Name<EventHandler<?>> {
}
