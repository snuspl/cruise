package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Evaluators with this size will be requested (in MBs)",
                short_name = "evalSize",
                default_value = "128")
public final class EvaluatorSize implements Name<Integer> {
}
