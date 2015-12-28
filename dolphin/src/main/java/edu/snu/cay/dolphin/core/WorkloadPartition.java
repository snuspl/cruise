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

import com.google.common.collect.Iterators;
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
 * A class for representing a workload partition assigned to each evaluator to process in iterations.
 * Each ComputeTask maintains its own local partition.
 * It can be updated by ControllerTask and retrieved by local ComputeTask.
 */
@EvaluatorSide
public final class WorkloadPartition {

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  /**
   * Typed data ids, which represent a workload partition assigned to a single ComputeTask.
   */
  private final ConcurrentMap<String, Set<LongRange>> typeToRanges;

  @Inject
  public WorkloadPartition() {
    this.typeToRanges = new ConcurrentHashMap<>();
  }

  /**
   * Put all entries of {@code workloadMap} into {@code typeToRanges}.
   * The initialization can be done only once.
   * ControllerTask or PreComputeTask initializes a local workload partition.
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
   * Returns all data types that this workload partition holds.
   * UserComputeTask invokes the method to get the data types that it has to process, when starting an iteration.
   * @return a set of types composing workload partition
   */
  public Iterator<String> getDataTypes() {
    if (!initialized.get()) {
      return Iterators.emptyIterator();
    } else {
      return new HashSet<>(typeToRanges.keySet()).iterator();
    }
  }

  /**
   * Returns workload partition of specific type.
   * UserComputeTask invokes the method to retrieve the workload partition, when starting an iteration.
   * @param dataType a type of data
   * @return a range set of dataType
   */
  public Iterator<LongRange> getRanges(final String dataType) {
    if (!initialized.get()) {
      return Iterators.emptyIterator();
    }

    final Set<LongRange> rangeSet = typeToRanges.get(dataType);
    if (rangeSet == null) {
      return Iterators.emptyIterator();
    } else {
      return new HashSet<>(rangeSet).iterator();
    }
  }

  /**
   * Returns the service configuration for the Workload Partition.
   * @return service configuration for the Workload Partition
   */
  public static Configuration getServiceConfiguration() {
    return ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, WorkloadPartition.class)
        .build();
  }
}
