package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Number of evaluators to run job with",
                short_name = "split")
public final class EvaluatorNum implements Name<Integer> {
}
