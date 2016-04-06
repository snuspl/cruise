/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.common.parameters.NumPartitions;
import edu.snu.cay.services.em.evaluator.api.PartitionFunc;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * An implementation of PartitionFunc.
 * It partitions ids with range-based partitioning, where partition p takes
 * keys within [p * PARTITION_SIZE, (p+1) * PARTITION_SIZE).
 */
public final class RangePartitionFunc implements PartitionFunc {

  private final int numPartitions;

  @Inject
  private RangePartitionFunc(@Parameter(NumPartitions.class) final int numPartitions) {
    this.numPartitions = numPartitions;
  }

  @Override
  public int getPartitionId(final long dataId) {
    final long partitionSize = Long.MAX_VALUE / numPartitions;
    return (int) (dataId / partitionSize);
  }

}
