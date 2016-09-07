package edu.snu.cay.services.ps.worker.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "Max number of pending pulls per WorkerThread", default_value = "5000",
    short_name = "max_pending_pulls_per_thread")
public class MaxPendingPullsPerThread implements Name<Integer> {
}
