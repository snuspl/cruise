package edu.snu.cay.services.em.examples.simple.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Period between iterations in milliseconds",
    default_value = "2000", short_name = "example_period_ms")
public final class PeriodMillis implements Name<Integer> {
}
