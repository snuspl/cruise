package edu.snu.reef.flexion.examples.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "maximum time allowed for the job to run, in milliseconds",
                short_name = "timeout",
                default_value = "100000")
public final class Timeout implements Name<Integer> {
}
