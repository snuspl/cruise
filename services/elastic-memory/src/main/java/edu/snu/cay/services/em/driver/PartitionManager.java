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
package edu.snu.cay.services.em.driver;

import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;

/**
 * Manager class for keeping track of partitions registered by evaluators.
 */
@DriverSide
public final class PartitionManager {

  private final Map<String, Map<String, TreeSet<LongRange>>> mapEvalDatatypeRanges;
  private final Map<String, TreeSet<LongRange>> globalDatatypeRanges;

  private Comparator<LongRange> longRangeComparator = new Comparator<LongRange>() {
    @Override
    public int compare(final LongRange o1, final LongRange o2) {
      return (int) ((o1.getMinimumLong() - o2.getMinimumLong()) + (o1.getMaximumLong() - o2.getMaximumLong()));
    }
  };

  @Inject
  private PartitionManager() {
    this.mapEvalDatatypeRanges = new HashMap<>();
    this.globalDatatypeRanges = new HashMap<>();
  }

  public boolean registerPartition(final String evalId,
                                final String dataType, final long unitStartId, final long unitEndId) {
    return registerPartition(evalId, dataType, new LongRange(unitStartId, unitEndId));
  }

  public synchronized boolean registerPartition(final String evalId, final String dataType, final LongRange idRange) {
    // 1. Check the acceptability of a new partition into globalDatatypeRanges
    final TreeSet<LongRange> rangeSetGlobal;

    if (globalDatatypeRanges.containsKey(dataType)) {
      // Check the overlap of registering partition to adjacent partitions across all the evaluators
      rangeSetGlobal = globalDatatypeRanges.get(dataType);
      final LongRange ceilingRange = rangeSetGlobal.ceiling(idRange);
      if (ceilingRange != null && ceilingRange.overlapsRange(idRange)) {
        return false; // upside overlaps
      }
      final LongRange floorRange = rangeSetGlobal.floor(idRange);
      if (floorRange != null && floorRange.overlapsRange(idRange)) {
        return false; // downside overlaps
      }
    } else {
      rangeSetGlobal = new TreeSet<>(longRangeComparator);
      globalDatatypeRanges.put(dataType, rangeSetGlobal);
    }

    // 2. Check the acceptability of a new partition into mapEvalDatatypeRanges
    final Map<String, TreeSet<LongRange>> evalDatatypeRanges;

    if (mapEvalDatatypeRanges.containsKey(evalId)) {
      evalDatatypeRanges = mapEvalDatatypeRanges.get(evalId);
    } else {
      evalDatatypeRanges = new HashMap<>();
      mapEvalDatatypeRanges.put(evalId, evalDatatypeRanges);
    }

    final TreeSet<LongRange> rangeSetEval;

    if (evalDatatypeRanges.containsKey(dataType)) {
      rangeSetEval = evalDatatypeRanges.get(dataType);
    } else {
      rangeSetEval = new TreeSet<>(longRangeComparator);
      evalDatatypeRanges.put(dataType, rangeSetEval);
    }

    // Check the registering partition's possibility to be merged to adjacent partitions within the evaluator
    // and then merge contiguous partitions into a big partition
    final LongRange higherRangeEval = rangeSetEval.higher(idRange);
    final long endId;

    if (higherRangeEval != null && higherRangeEval.getMinimumLong() == idRange.getMaximumLong() + 1) {
      rangeSetGlobal.remove(higherRangeEval);
      rangeSetEval.remove(higherRangeEval);
      endId = higherRangeEval.getMaximumLong();
    } else {
      endId = idRange.getMaximumLong();
    }

    final LongRange lowerRangeEval = rangeSetEval.lower(idRange);
    final long startId;

    if (lowerRangeEval != null && lowerRangeEval.getMaximumLong() + 1 == idRange.getMinimumLong()) {
      rangeSetGlobal.remove(lowerRangeEval);
      rangeSetEval.remove(lowerRangeEval);
      startId = lowerRangeEval.getMinimumLong();
    } else {
      startId = idRange.getMinimumLong();
    }

    final LongRange mergedRange = new LongRange(startId, endId);

    rangeSetGlobal.add(mergedRange);
    rangeSetEval.add(mergedRange);

    return true;
  }

  public synchronized Set<LongRange> getRangeSet(final String evalId, final String dataType) {
    if (!mapEvalDatatypeRanges.containsKey(evalId)) {
      return new TreeSet<>();
    }

    final Map<String, TreeSet<LongRange>> mapDatatypeRange = mapEvalDatatypeRanges.get(evalId);
    if (!mapDatatypeRange.containsKey(dataType)) {
      return new TreeSet<>();
    }

    return new TreeSet<>(mapDatatypeRange.get(dataType));
  }

  // TODO #122: Handle a request to remove the merged partitions. There are two possible choices:
  // 1) Do not accept the request
  // 2) Split the big partition into several smaller partitions again and remove the requested partition
  public synchronized boolean remove(final String evalId, final String dataType, final LongRange longRange) {
    final Map<String, TreeSet<LongRange>> evalDatatypeRanges = mapEvalDatatypeRanges.get(evalId);
    if (evalDatatypeRanges == null) {
      return false;
    }

    final TreeSet<LongRange> rangeSet = evalDatatypeRanges.get(dataType);
    if (rangeSet == null) {
      return false;
    }

    return rangeSet.remove(longRange);
  }
}
