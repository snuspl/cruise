package edu.snu.reef.flexion.groupcomm.names;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name of the single communication group used for k-means job")
public final class CommunicationGroup implements Name<String> {
}
