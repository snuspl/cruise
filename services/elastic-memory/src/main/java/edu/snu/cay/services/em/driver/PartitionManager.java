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

  private final Map<String, Map<String, TreeSet<LongRange>>> evalDataTypeRanges;
  private final Map<String, TreeSet<LongRange>> globalDataTypeRanges;

  /**
   * If ranges are not overlapped, we can always compare the two ranges, so the ranges
   * are ordered in the TreeSet. On the other hand, we have to handle the overlapping
   * ranges to guarantee the total ordering of elements.
   */
  private static final Comparator<LongRange> LONG_RANGE_COMPARATOR = new Comparator<LongRange>() {
    @Override
    public int compare(final LongRange o1, final LongRange o2) {
      if (o1.getMaximumLong() < o2.getMinimumLong()) {
        return -1;
      } else if (o1.getMinimumLong() > o2.getMaximumLong()) {
        return 1;
      } else {
        return 0;
      }
    }
  };

  @Inject
  private PartitionManager() {
    this.evalDataTypeRanges = new HashMap<>();
    this.globalDataTypeRanges = new HashMap<>();
  }

  /**
   * Add a partition to an evaluator.
   * @param evalId Identifier of the Evaluator.
   * @param dataType Type of the data.
   * @param unitStartId Smallest unit id in the partition.
   * @param unitEndId Largest unit id in the partition.
   * @return {@code true} if the partition is registered successfully, {@code false} otherwise.
   */
  public boolean registerPartition(final String evalId, final String dataType,
                                   final long unitStartId, final long unitEndId) {
    return registerPartition(evalId, dataType, new LongRange(unitStartId, unitEndId));
  }

  public synchronized boolean registerPartition(final String evalId, final String dataType, final LongRange idRange) {
    // Check the acceptability of a new partition into globalDataTypeRanges.
    // Early failure in this step guarantees that all ranges are unique across evaluators.
    final TreeSet<LongRange> rangeSetGlobal;

    if (globalDataTypeRanges.containsKey(dataType)) {
      rangeSetGlobal = globalDataTypeRanges.get(dataType);
      final LongRange ceilingRange = rangeSetGlobal.ceiling(idRange);
      if (ceilingRange != null && ceilingRange.overlapsRange(idRange)) {
        return false; // upside overlaps
      }
      final LongRange floorRange = rangeSetGlobal.floor(idRange);
      if (floorRange != null && floorRange.overlapsRange(idRange)) {
        return false; // downside overlaps
      }
    } else {
      // Successfully registered in the global partitions.
      rangeSetGlobal = new TreeSet<>(LONG_RANGE_COMPARATOR);
      globalDataTypeRanges.put(dataType, rangeSetGlobal);
    }

    // Check the acceptability of a new partition into evalDataTypeRanges
    final Map<String, TreeSet<LongRange>> evalPartitions;

    if (this.evalDataTypeRanges.containsKey(evalId)) {
      evalPartitions = this.evalDataTypeRanges.get(evalId);
    } else {
      evalPartitions = new HashMap<>();
      this.evalDataTypeRanges.put(evalId, evalPartitions);
    }

    final TreeSet<LongRange> rangeSetEval;

    if (evalPartitions.containsKey(dataType)) {
      rangeSetEval = evalPartitions.get(dataType);
    } else {
      rangeSetEval = new TreeSet<>(LONG_RANGE_COMPARATOR);
      evalPartitions.put(dataType, rangeSetEval);
    }

    // Check the registering partition's possibility to be merged to adjacent partitions within the evaluator
    // and then merge contiguous partitions into a big partition.
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

  /**
   * Get the existing range set in a evaluator of a type.
   * @return Sorted set of ranges. An empty set is returned if there is no matched range.
   */
  public synchronized Set<LongRange> getRangeSet(final String evalId, final String dataType) {
    if (!evalDataTypeRanges.containsKey(evalId)) {
      return new TreeSet<>();
    }

    final Map<String, TreeSet<LongRange>> mapDatatypeRange = evalDataTypeRanges.get(evalId);
    if (!mapDatatypeRange.containsKey(dataType)) {
      return new TreeSet<>();
    }

    return new TreeSet<>(mapDatatypeRange.get(dataType));
  }

  /**
   * Remove the range from the partitions. After deleting a range, the partitions could be rearranged.
   * @param evalId Identifier of Evaluator.
   * @param dataType Type of data.
   * @param longRange Range to remove.
   * @return Ranges that are removed from the partitions. If the entire range is not matched, part of range
   * is removed and returned. On the other hand, multiple ranges could be deleted if the range contains multiple
   * partitions. An empty set is returned if there is no intersecting range.
   */
  public synchronized TreeSet<LongRange> remove(final String evalId, final String dataType, final LongRange longRange) {
    // Early failure if the evaluator is empty.
    final Map<String, TreeSet<LongRange>> evalPartitions = this.evalDataTypeRanges.get(evalId);
    if (evalPartitions == null) {
      return new TreeSet<>();
    }

    // Early failure if the evaluator does not have any data of that type.
    final TreeSet<LongRange> evalRangeSet = evalPartitions.get(dataType);
    if (evalRangeSet == null) {
      return new TreeSet<>();
    }

    final TreeSet<LongRange> globalRanges = this.globalDataTypeRanges.get(dataType);
    final TreeSet<LongRange> removedRanges = new TreeSet<>(LONG_RANGE_COMPARATOR);

    final Set<LongRange> insideRanges = removeInsideRanges(globalRanges, evalRangeSet, longRange);
    removedRanges.addAll(insideRanges);

    // Remove overlapping ranges if any. Try ceilingRange first.
    final LongRange ceilingRange = evalRangeSet.ceiling(longRange);
    if (ceilingRange != null) {
      final LongRange ceilingRemoved = removeIntersectingRange(globalRanges, evalRangeSet, ceilingRange, longRange);
      if (ceilingRemoved != null) {
        removedRanges.add(ceilingRemoved);
      }
    }

    // Next try the floorRange. There is no duplicate removal because we already removed ceiling range.
    final LongRange floorRange = evalRangeSet.floor(longRange);
    if (floorRange != null) {
      final LongRange floorRemoved = removeIntersectingRange(globalRanges, evalRangeSet, floorRange, longRange);
      if (floorRemoved != null) {
        removedRanges.add(floorRemoved);
      }
    }

    return removedRanges;
  }

  /**
   * Remove ranges which target range contains whole range.
   * (e.g., [3, 4] [5, 6] when [1, 10] is requested to remove).
   * @return Ranges that are removed. An empty list is returned if there was no range to remove.
   */
  private Set<LongRange> removeInsideRanges(final TreeSet<LongRange> globalRanges,
                                            final TreeSet<LongRange> evalRanges,
                                            final LongRange target) {
    final long min = target.getMinimumLong();
    final long max = target.getMaximumLong();

    final NavigableSet<LongRange> insideRanges =
        evalRanges.subSet(new LongRange(min, min), false, new LongRange(max, max), false);

    // If we do not copy the ranges, we lose them because the references are removed from the set.
    final NavigableSet<LongRange> copied = new TreeSet<>(insideRanges);
    removeAll(evalRanges, insideRanges);
    removeAll(globalRanges, insideRanges);
    return copied;
  }

  /**
   * Remove the (sub)range from one range.
   * @param from Original range
   * @param target Target range to remove
   * @return Deleted range. {@code null} if there is no range to delete.
   */
  private LongRange removeIntersectingRange(final TreeSet<LongRange> globalRanges,
                                            final TreeSet<LongRange> evalRanges,
                                            final LongRange from,
                                            final LongRange target) {
    // Remove the range temporarily.
    evalRanges.remove(from);
    globalRanges.remove(from);

    if (target.containsRange(from)) {
      // If the target range is larger, the whole range is removed.
      return from;

    } else if (from.containsRange(target)) {
      // If the original range is larger, need repartition
      // Split the original range outside target in 2 pieces, and insert them
      final LongRange smaller = new LongRange(from.getMinimumLong(), target.getMinimumLong() - 1);
      final LongRange bigger = new LongRange(target.getMaximumLong() + 1, from.getMaximumLong());
      globalRanges.add(smaller);
      globalRanges.add(bigger);
      evalRanges.add(smaller);
      evalRanges.add(bigger);
      return target;

    } else if (from.getMinimumLong() < target.getMinimumLong()){
      // Partially overlapping ranges will remove intersection only.
      // Reinsert the rest of the range.
      // min(from) < min(target) < max(from) < max(target)
      final LongRange toRemove = new LongRange(target.getMinimumLong(), from.getMaximumLong());
      final LongRange toReInsert = new LongRange(from.getMinimumLong(), target.getMinimumLong() - 1);
      evalRanges.add(toReInsert);
      globalRanges.add(toReInsert);
      return toRemove;

    } else if (target.getMinimumLong() < from.getMinimumLong()) {
      // min(target) < min(from) < max(target) < max(from)
      final LongRange toRemove = new LongRange(from.getMinimumLong(), target.getMaximumLong());
      final LongRange toReInsert = new LongRange(target.getMaximumLong() + 1, from.getMaximumLong());
      evalRanges.add(toReInsert);
      globalRanges.add(toReInsert);
      return toRemove;
    } else {
      throw new RuntimeException("Might be an unhandled corner case.");
    }
  }

  /**
   * Helper method that removes all the original from the range set. Since {@code Set.removeAll()} throws
   * {@link ConcurrentModificationException}, iterator is used instead.
   */
  private void removeAll(final TreeSet<LongRange> original, final Set<LongRange> toRemove) {
    final Iterator<LongRange> iterator = original.iterator();
    while (iterator.hasNext()) {
      final LongRange range = iterator.next();
      if (toRemove.contains(range)) {
        iterator.remove();
      }
    }
  }
}
