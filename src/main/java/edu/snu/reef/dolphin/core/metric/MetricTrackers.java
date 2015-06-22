package edu.snu.reef.dolphin.core.metric;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * The set of MetricTracker implementations registered to MetricManager.
 */
@NamedParameter(doc = "The set of MetricTracker implementations registered to MetricManager.",
    default_classes = {})
public final class MetricTrackers implements Name<Set<MetricTracker>> {
}
