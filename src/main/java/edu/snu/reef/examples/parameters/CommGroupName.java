package edu.snu.reef.examples.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name used for configuration group communication")
public final class CommGroupName implements Name<String> {
}
