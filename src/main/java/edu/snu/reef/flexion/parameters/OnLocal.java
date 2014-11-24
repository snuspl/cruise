package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Whether or not to run on local machine",
                short_name = "local",
                default_value = "false")
public final class OnLocal implements Name<Boolean> {
}
