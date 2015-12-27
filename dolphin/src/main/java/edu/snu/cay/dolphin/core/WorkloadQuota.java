/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.core;

import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class for representing a workload quota assigned to each evaluator to process in iterations.
 * Each ComputeTask maintains its own local quota.
 * It can be updated by ControllerTask and retrieved by local ComputeTask.
 */
@EvaluatorSide
public final class WorkloadQuota {

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * Typed data ids, which represent a workload quota assigned to a single ComputeTask.
   */
  private final ConcurrentMap<String, Set<LongRange>> typeToRanges;

  @Inject
  public WorkloadQuota() {
    this.typeToRanges = new ConcurrentHashMap<>();
  }

  /**
   * ControllerTask or PreComputeTask initializes a local workload quota.
   * Put all entries of {@code workloadMap} into {@code typeToRanges}.
   * The initialization can be done only once.
   * @param workloadMap initially assigned to this task
   * @return true when successfully initialized
   */
  public boolean initialize(final Map<String, Set<LongRange>> workloadMap) {
    if (workloadMap == null) {
      return false;
    }
    if (!initialized.compareAndSet(false, true)) {
      return false;
    }

    typeToRanges.putAll(workloadMap);
    return true;
  }

  /**
   * UserComputeTask gets own workload quota when starting an iteration.
   * @return a workload map assigned to this task
   */
  public Map<String, Set<LongRange>> getAll() {
    return Collections.unmodifiableMap(typeToRanges);
  }

  /**
   * UserComputeTask gets own workload quota of specific type when starting an iteration.
   * @param dataType a type of data
   * @return a range set of dataType
   */
  public Set<LongRange> get(final String dataType) {
    final Set<LongRange> rangeset = typeToRanges.get(dataType);
    if (rangeset == null) {
      return new HashSet<>();
    } else {
      return Collections.unmodifiableSet(rangeset);
    }
  }

  /**
   * Returns the service configuration for the Workload Quota.
   * @return service configuration for the Workload Quota
   */
  public static Configuration getServiceConfiguration() {
    return ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, WorkloadQuota.class)
        .build();
  }
}
