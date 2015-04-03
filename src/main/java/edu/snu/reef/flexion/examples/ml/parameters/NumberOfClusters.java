package edu.snu.reef.flexion.examples.ml.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Number of clusters",
        short_name = "numCls",
        default_value = "5")
public final class NumberOfClusters implements Name<Integer> {

}
