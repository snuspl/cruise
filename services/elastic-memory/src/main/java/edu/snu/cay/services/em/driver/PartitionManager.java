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

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager class for keeping track of partitions registered by evaluators.
 * It handles a global view of the partitions of all Evaluators, so we can guarantee that all partitions are unique.
 * This class is thread-safe, so many users can request to register/remove/move partitions at the same time.
 */
@ThreadSafe
@DriverSide
public final class PartitionManager {
  private static final Logger LOG = Logger.getLogger(PartitionManager.class.getName());

  /**
   * This map holds the partitions of all evaluators by using data type as a key.
   * We can easily check the uniqueness of a partition when we register it.
   * Note that we should update the both maps when adding or removing a partition.
   */
  private final Map<String, TreeSet<LongRange>> globalPartitions;

  /**
   * This map consists of the sub-map which holds the partitions of each evaluator.
   * When asked to remove/move partitions, we choose the partitions based on this information.
   * For example, let's assume that Evaluator1 has [1, 4] and Evaluator2 has [5, 8].
   * When {@code remove[3, 6]} is requested to Evaluator1, then only [5, 6] will be removed because
   * Evaluator2 does not have rest of the partitions.
   * Note that we should update the both maps when adding or removing a partition.
   */
  private final Map<String, Map<String, TreeSet<LongRange>>> evalPartitions;

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
    this.evalPartitions = new HashMap<>();
    this.globalPartitions = new HashMap<>();
  }

  /**
   * Register a partition to an evaluator.
   * @param evalId Identifier of the Evaluator.
   * @param dataType Type of the data.
   * @param unitStartId Smallest unit id in the partition.
   * @param unitEndId Largest unit id in the partition.
   * @return {@code true} if the partition is registered successfully, {@code false} otherwise.
   */
  public boolean register(final String evalId, final String dataType,
                          final long unitStartId, final long unitEndId) {
    return register(evalId, dataType, new LongRange(unitStartId, unitEndId));
  }

  /**
   * Register a partition to an evaluator.
   * @param evalId Identifier of the Evaluator.
   * @param dataType Type of the data.
   * @param idRange Range of id
   * @return {@code true} if the partition is registered successfully, {@code false} otherwise.
   */
  public synchronized boolean register(final String evalId, final String dataType, final LongRange idRange) {
    // Check the acceptability of a new partition into globalPartitions.
    // Early failure in this step guarantees that all ranges are unique across evaluators.
    final TreeSet<LongRange> globalRanges;

    if (globalPartitions.containsKey(dataType)) {
      globalRanges = globalPartitions.get(dataType);
      final LongRange ceilingRange = globalRanges.ceiling(idRange);
      if (ceilingRange != null && ceilingRange.overlapsRange(idRange)) {
        return false; // upside overlaps
      }
      final LongRange floorRange = globalRanges.floor(idRange);
      if (floorRange != null && floorRange.overlapsRange(idRange)) {
        return false; // downside overlaps
      }
    } else {
      // Successfully registered in the global partitions.
      globalRanges = new TreeSet<>(LONG_RANGE_COMPARATOR);
      globalPartitions.put(dataType, globalRanges);
    }

    // Check the acceptability of a new partition into evalPartitions
    final Map<String, TreeSet<LongRange>> evalDataTypeRanges;

    if (this.evalPartitions.containsKey(evalId)) {
      evalDataTypeRanges = this.evalPartitions.get(evalId);
    } else {
      evalDataTypeRanges = new HashMap<>();
      this.evalPartitions.put(evalId, evalDataTypeRanges);
    }

    final TreeSet<LongRange> evalRanges;

    if (evalDataTypeRanges.containsKey(dataType)) {
      evalRanges = evalDataTypeRanges.get(dataType);
    } else {
      evalRanges = new TreeSet<>(LONG_RANGE_COMPARATOR);
      evalDataTypeRanges.put(dataType, evalRanges);
    }

    // Check the registering partition's possibility to be merged to adjacent partitions within the evaluator
    // and then merge contiguous partitions into a big partition.
    final LongRange higherRange = evalRanges.higher(idRange);
    final long endId;

    if (higherRange != null && higherRange.getMinimumLong() == idRange.getMaximumLong() + 1) {
      globalRanges.remove(higherRange);
      evalRanges.remove(higherRange);
      endId = higherRange.getMaximumLong();
    } else {
      endId = idRange.getMaximumLong();
    }

    final LongRange lowerRange = evalRanges.lower(idRange);
    final long startId;

    if (lowerRange != null && lowerRange.getMaximumLong() + 1 == idRange.getMinimumLong()) {
      globalRanges.remove(lowerRange);
      evalRanges.remove(lowerRange);
      startId = lowerRange.getMinimumLong();
    } else {
      startId = idRange.getMinimumLong();
    }

    final LongRange mergedRange = new LongRange(startId, endId);

    globalRanges.add(mergedRange);
    evalRanges.add(mergedRange);

    return true;
  }

  /**
   * Get the existing range set in a evaluator of a type.
   * @return Sorted set of ranges. An empty set is returned if there is no matched range.
   */
  public synchronized Set<LongRange> getRangeSet(final String evalId, final String dataType) {
    if (!evalPartitions.containsKey(evalId)) {
      LOG.log(Level.WARNING, "The evaluator {0} does not exist.", evalId);
      return new TreeSet<>();
    }

    final Map<String, TreeSet<LongRange>> evalDataTypeRanges = evalPartitions.get(evalId);
    if (!evalDataTypeRanges.containsKey(dataType)) {
      LOG.log(Level.WARNING, "The evaluator {0} does not contain any data whose type is {1}.",
          new Object[]{evalId, dataType});
      return new TreeSet<>();
    }

    return new TreeSet<>(evalDataTypeRanges.get(dataType));
  }

  /**
   * Remove the range from the partitions. After deleting a range, the partitions could be rearranged.
   * @param evalId Identifier of Evaluator.
   * @param dataType Type of data.
   * @param idRange Range of unit ids to remove.
   * @return Ranges that are removed from the partitions. If the entire range is not matched, part of range
   * is removed and returned. On the other hand, multiple ranges could be deleted if the range contains multiple
   * partitions. An empty set is returned if there is no intersecting range.
   */
  public synchronized TreeSet<LongRange> remove(final String evalId, final String dataType, final LongRange idRange) {
    // Early failure if the evaluator is empty.
    final Map<String, TreeSet<LongRange>> evalDataTypeRanges = evalPartitions.get(evalId);
    if (evalDataTypeRanges == null) {
      LOG.log(Level.WARNING, "The evaluator {0} does not exist.", evalId);
      return new TreeSet<>();
    }

    // Early failure if the evaluator does not have any data of that type.
    final TreeSet<LongRange> evalRanges = evalDataTypeRanges.get(dataType);
    if (evalRanges == null) {
      LOG.log(Level.WARNING, "The evaluator {0} does not contain any data whose type is {1}.",
          new Object[]{evalId, dataType});
      return new TreeSet<>();
    }

    final TreeSet<LongRange> globalRanges = globalPartitions.get(dataType);
    final TreeSet<LongRange> removedRanges = new TreeSet<>(LONG_RANGE_COMPARATOR);

    final Set<LongRange> insideRanges = removeInsideRanges(globalRanges, evalRanges, idRange);
    removedRanges.addAll(insideRanges);

    // Remove overlapping ranges if any. Try ceilingRange first.
    final LongRange ceilingRange = evalRanges.ceiling(idRange);
    if (ceilingRange != null && ceilingRange.overlapsRange(idRange)) {
      final LongRange ceilingRemoved = removeIntersectingRange(globalRanges, evalRanges, ceilingRange, idRange);
      if (ceilingRemoved != null) {
        removedRanges.add(ceilingRemoved);
      }
    }

    // Next try the floorRange. There is no duplicate removal because we already removed ceiling range.
    final LongRange floorRange = evalRanges.floor(idRange);
    if (floorRange != null && floorRange.overlapsRange(idRange)) {
      final LongRange floorRemoved = removeIntersectingRange(globalRanges, evalRanges, floorRange, idRange);
      if (floorRemoved != null) {
        removedRanges.add(floorRemoved);
      }
    }

    return removedRanges;
  }

  /**
   * Move a partition to another evaluator.
   * @param srcId Id of the source.
   * @param destId Id of the destination.
   * @param dataType Type of the data.
   * @return {@code true} if the move was successful.
   */
  public synchronized boolean move(final String srcId,
                                   final String destId,
                                   final String dataType,
                                   final LongRange toMove) {
    final TreeSet<LongRange> removed = remove(srcId, dataType, toMove);
    for (final LongRange toRegister : removed) {
      final boolean succeeded = register(destId, dataType, toRegister);
      if (!succeeded) {
        return false;
      }
    }
    return true;
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

    // Copy the ranges to avoid ConcurrentModificationException and losing references.
    final NavigableSet<LongRange> copied = new TreeSet<>(insideRanges);
    evalRanges.removeAll(copied);
    globalRanges.removeAll(copied);
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
    // Remove the range from both global and evaluator's range sets.
    removeRange(from, globalRanges, evalRanges);

    if (target.containsRange(from)) {
      // If the target range is larger, the whole range is removed.
      return from;

    } else {
      // If two sections are overlapping, we can divide into three parts: LEFT | CENTER | RIGHT
      // We need to remove CENTER ([centerLeft centerRight]) which is an intersection, and keep
      // LEFT ([leftEnd (centerLeft-1)] and RIGHT ([(centerRight+1) rightEnd]) if they are not empty.
      final long minFrom = from.getMinimumLong();
      final long maxFrom = from.getMaximumLong();
      final long minTarget = target.getMinimumLong();
      final long maxTarget = target.getMaximumLong();

      final long leftEnd = Math.min(minFrom, minTarget);
      final long centerLeft = Math.max(minFrom, minTarget);
      final long centerRight = Math.min(maxFrom, maxTarget);
      final long endRight = Math.max(maxFrom, maxTarget);

      // Keep LEFT if exists
      if (leftEnd < centerLeft) {
        final LongRange left = new LongRange(leftEnd, centerLeft - 1);
        addRange(left, globalRanges, evalRanges);
      }

      // Keep RIGHT if exists
      if (endRight > centerRight) {
        final LongRange right = new LongRange(centerRight + 1, endRight);
        addRange(right, globalRanges, evalRanges);
      }

      return target;
    }
  }

  /**
   * Helper method for removing a range from both global and evaluator's range sets.
   * For convenience, the range sets are passed via arguments to avoid additional lookup.
   */
  private void removeRange(final LongRange toRemove,
                           final TreeSet<LongRange> globalRange,
                           final TreeSet<LongRange> evalRange) {
    globalRange.remove(toRemove);
    evalRange.remove(toRemove);
  }

  /**
   * Helper method for adding a range from both global and evaluator's range sets.
   * For convenience, the range sets are passed via arguments to avoid additional lookup.
   */
  private void addRange(final LongRange toAdd,
                        final TreeSet<LongRange> globalRange,
                        final TreeSet<LongRange> evalRange) {
    globalRange.add(toAdd);
    evalRange.add(toAdd);
  }
}
