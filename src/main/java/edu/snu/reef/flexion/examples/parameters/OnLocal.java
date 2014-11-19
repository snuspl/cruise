package edu.snu.reef.flexion.examples.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Whether or not to run on local runtime",
                short_name = "local",
                default_value = "true")
public final class OnLocal implements Name<Boolean> {
}
