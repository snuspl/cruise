package edu.snu.cay.services.em.examples.simple.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Number of iterations to run move back and forth",
    default_value = "1", short_name = "example_iterations")
public final class Iterations implements Name<Integer> {
}
