package edu.snu.reef.flexion.groupcomm.names;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name for the operation used to reduce data")
public final class DataGather implements Name<String> {
}
