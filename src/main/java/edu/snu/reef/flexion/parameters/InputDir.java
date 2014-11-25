package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "File or directory to read input data from",
                short_name = "inputDir")
public final class InputDir implements Name<String> {
}
