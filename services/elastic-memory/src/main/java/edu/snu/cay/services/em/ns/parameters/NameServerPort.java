package edu.snu.cay.services.em.ns.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Name server port for NSWrapper")
public class NameServerPort implements Name<Integer> {
}
