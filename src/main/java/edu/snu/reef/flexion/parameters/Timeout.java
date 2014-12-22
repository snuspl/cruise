package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Time allowed until job ends",
                short_name = "timeout",
                default_value = "100000")
public final class Timeout implements Name<Integer> {
}
