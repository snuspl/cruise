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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class for representing a workload quota assigned to each evaluator to process in iterations.
 * Each ComputeTask maintain its own local quota.
 * It can be updated by ControllerTask and retrieved by ComputeTasks.
 */
@EvaluatorSide
public final class WorkloadQuota {
  private static final Logger LOG = Logger.getLogger(WorkloadQuota.class.getName());

  /**
   * Typed data ids, which represent a workload quota assigned to a single computeTask.
   */
  private final Map<String, Set<LongRange>> typeToRanges;

  private final ReadWriteLock readWriteLock;

  @Inject
  public WorkloadQuota() {
    this.typeToRanges = new HashMap<>();
    this.readWriteLock = new ReentrantReadWriteLock(true);
  }

  /**
   * PreComputeTask initializes a local workload quota.
   */
  public void register(final String dataType, final long startId, final long endId) {
    readWriteLock.writeLock().lock();
    final Set<LongRange> rangeSet = typeToRanges.get(dataType);
    if (rangeSet == null) {
      final Set<LongRange> freshSet = new HashSet<>();
      freshSet.add(new LongRange(startId, endId));
      typeToRanges.put(dataType, freshSet);
      LOG.log(Level.INFO, "Create and put a range set of {0} type into typeToRanges.", dataType);
    } else {
      rangeSet.add(new LongRange(startId, endId));
    }
    LOG.log(Level.INFO, "Add a range[{0}, {1}) to a set of {2} type", new Object[]{startId, endId, dataType});
    readWriteLock.writeLock().unlock();
  }

  /**
   * UserComputeTask gets own workload quota when starting an iteration.
   * @return
   */
  public Map<String, Set<LongRange>> getAll() {
    return typeToRanges;
  }

  /**
   * UserComputeTask gets own workload quota of specific type when starting an iteration.
   * @param dataType
   * @return
   */
  public Set<LongRange> get(final String dataType) {
    return typeToRanges.get(dataType);
  }

  public static Configuration getServiceConfiguration() {
    return ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, WorkloadQuota.class)
        .build();
  }
}
