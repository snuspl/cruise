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

  private final Map<String, Map<String, TreeSet<LongRange>>> mapIdDatatypeRange;
  private final Map<String, TreeSet<LongRange>> globalDatatypeRanges;

  private Comparator<LongRange> longRangeComparator = new Comparator<LongRange>() {
    @Override
    public int compare(final LongRange o1, final LongRange o2) {
      return (int) (o1.getMinimumLong() - o2.getMinimumLong());
    }
  };

  @Inject
  private PartitionManager() {
    this.mapIdDatatypeRange = new HashMap<>();
    this.globalDatatypeRanges = new HashMap<>();
  }

  public boolean registerPartition(final String evalId,
                                final String dataType, final long unitStartId, final long unitEndId) {
    return registerPartition(evalId, dataType, new LongRange(unitStartId, unitEndId));
  }

  public synchronized boolean registerPartition(final String evalId, final String dataType, final LongRange idRange) {
    // 1. Check the acceptability of a new partition into globalDatatypeRanges
    TreeSet<LongRange> rangeSetGlobal = globalDatatypeRanges.get(dataType);
    if (rangeSetGlobal != null) {
      if (rangeSetGlobal.contains(idRange)) {
        return false;
      }
    } else {
      rangeSetGlobal = new TreeSet<>(longRangeComparator);
      assert (globalDatatypeRanges.put(dataType, rangeSetGlobal) == null);
    }

    // 1-1. Check the overlap of registering partition to adjacent partitions across all the evaluators
    final LongRange higherRange = rangeSetGlobal.higher(idRange);
    if (higherRange != null && higherRange.overlapsRange(idRange)) {
      return false; // upside (or may also downside) overlap(s)
    }
    final LongRange lowerRange = rangeSetGlobal.lower(idRange);
    if (lowerRange != null && lowerRange.overlapsRange(idRange)) {
      return false; // downside overlap(s)
    }

    // 2. Check the acceptability of a new partition into mapIdDatatypeRange
    Map<String, TreeSet<LongRange>> evalDatatypeRanges = mapIdDatatypeRange.get(evalId);

    if (evalDatatypeRanges == null) {
      evalDatatypeRanges = new HashMap<>();
      assert (mapIdDatatypeRange.put(evalId, evalDatatypeRanges) == null);
    }

    TreeSet<LongRange> rangeSetEval = evalDatatypeRanges.get(dataType);

    if (rangeSetEval == null) {
      rangeSetEval = new TreeSet<>(longRangeComparator);
      assert (evalDatatypeRanges.put(dataType, rangeSetEval) == null);
    }

    // 2-1. Check the registering partition's possibility to be merged to adjacent partitions within the evaluator
    boolean upsideMerge = false;
    boolean downsideMerge = false;
    final LongRange higherRangeEval = rangeSetEval.higher(idRange);
    final LongRange lowerRangeEval = rangeSetEval.lower(idRange);
    if (higherRangeEval != null && higherRangeEval.getMinimumLong() == idRange.getMaximumLong() + 1) {
      upsideMerge = true;
    }
    if (lowerRangeEval != null && lowerRangeEval.getMaximumLong() + 1 == idRange.getMinimumLong()) {
      downsideMerge = true;
    }

    // merge partitions
    LongRange mergedRange = idRange;

    if (upsideMerge || downsideMerge) {
      final long startId = downsideMerge ? lowerRangeEval.getMinimumLong() : idRange.getMinimumLong();
      final long endId = upsideMerge ? higherRangeEval.getMaximumLong() : idRange.getMaximumLong();
      mergedRange = new LongRange(startId, endId);

      if (downsideMerge) {
        assert (rangeSetGlobal.remove(lowerRangeEval));
        assert (rangeSetEval.remove(lowerRangeEval));
      }
      if (upsideMerge) {
        assert (rangeSetGlobal.remove(higherRangeEval));
        assert (rangeSetEval.remove(higherRangeEval));
      }
    }

    if (rangeSetGlobal.add(mergedRange)) {
      if (rangeSetEval.add(mergedRange)) {
        return true;
      } else { // rollback when registering fails
        assert (rangeSetGlobal.remove(mergedRange));

        if (downsideMerge) {
          assert (rangeSetGlobal.add(lowerRangeEval));
          assert (rangeSetEval.add(lowerRangeEval));
        }
        if (upsideMerge) {
          assert (rangeSetGlobal.add(higherRangeEval));
          assert (rangeSetEval.add(higherRangeEval));
        }
      }
    }

    return false;
  }

  public synchronized Set<LongRange> getRangeSet(final String evalId, final String dataType) {
    if (!mapIdDatatypeRange.containsKey(evalId)) {
      return null;
    }

    final Map<String, TreeSet<LongRange>> mapDatatypeRange = mapIdDatatypeRange.get(evalId);
    if (!mapDatatypeRange.containsKey(dataType)) {
      return null;
    }

    return new TreeSet<>(mapDatatypeRange.get(dataType));
  }

  /* TODO #122: handle a try of removing to merged partitions. split merged partition again? */
  public synchronized boolean remove(final String evalId, final String dataType, final LongRange longRange) {
    if (!mapIdDatatypeRange.containsKey(evalId)) {
      return false;
    }

    final Map<String, TreeSet<LongRange>> evalDatatypeRanges = mapIdDatatypeRange.get(evalId);
    if (evalDatatypeRanges == null) {
      return false;
    }
    final TreeSet<LongRange> rangeSet = evalDatatypeRanges.get(dataType);
    if (rangeSet == null) {
      return false;
    }

    return rangeSet.remove(longRange); // Note: TreeSet does not distinguish the difference in maximum value of range
  }
}
