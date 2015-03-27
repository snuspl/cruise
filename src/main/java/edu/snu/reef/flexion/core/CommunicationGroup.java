package edu.snu.reef.flexion.core;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name of the single communication group")
public final class CommunicationGroup implements Name<String> {
}
