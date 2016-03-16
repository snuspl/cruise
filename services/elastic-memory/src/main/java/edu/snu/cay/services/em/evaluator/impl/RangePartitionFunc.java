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

import edu.snu.cay.services.em.evaluator.api.PartitionFunc;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * An implementation of PartitionFunc.
 * It partitions ids with range-based partitioning.
 */
public final class RangePartitionFunc implements PartitionFunc {

  private final int partitionSizeBits;

  @Inject
  private RangePartitionFunc(@Parameter(PartitionSizeBits.class) final int partitionSizeBits) {
    if (partitionSizeBits <= 0 || partitionSizeBits > 32) {
      throw new RuntimeException("PartitionSizeBits should be a positive value no greater than 32");
    }
    this.partitionSizeBits = partitionSizeBits;
  }

  @Override
  public int partition(final long dataId) {
    return (int) (dataId >> partitionSizeBits);
  }

  @NamedParameter(doc = "A number of bits representing partition size", default_value = "32")
  public final class PartitionSizeBits implements Name<Integer> {
  }
}
