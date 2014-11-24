package edu.snu.reef.flexion.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name of job",
                short_name = "jobName")
public final class JobIdentifier implements Name<String> {
}
