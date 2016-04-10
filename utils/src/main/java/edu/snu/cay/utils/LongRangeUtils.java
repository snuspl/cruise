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
package edu.snu.cay.utils;

import org.apache.commons.lang.math.LongRange;

import java.util.Comparator;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Utilities for dealing with {@code LongRange}s.
 */
public final class LongRangeUtils {

  /**
   * Should not be instantiated.
   */
  private LongRangeUtils() {
  }

  /**
   * Comparator that is used when comparing the two long ranges.
   * It guarantees the total order if the ranges are disjoint,
   * but when the ranges are overlapping, the order is not determined..
   */
  public static final Comparator<LongRange> LONG_RANGE_COMPARATOR =
      new Comparator<LongRange>() {
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

  /**
   * Create an empty navigable set that we can put LongRanges.
   * The ranges are ordered in the set based on the following rule:
   *   1) If max(range1) < min(range2), then range1 < range2 because all the values
   *   in range1 is smaller than the numbers in range2.
   *   2) If min(range1) > max(range2), then range1 > range2.
   *   3) Otherwise, the order between two ranges are not clear. So users need more caution
   *   when handling this case.
   * @return An empty navigable set of LongRanges.
   */
  public static NavigableSet<LongRange> createLongRangeSet() {
    return new TreeSet<>(LONG_RANGE_COMPARATOR);
  }

  /**
   * Create a {@code Set} of {@code LongRange}s using a {@code SortedSet} of {@code Long}s.
   * This method tries to create as few {@code LongRange}s as possible, given that each of them must be dense.
   */
  public static Set<LongRange> generateDenseLongRanges(final SortedSet<Long> longSortedSet) {
    final Set<LongRange> longRangeSet = new HashSet<>();

    Long anchorLong = null;
    Long prevLong = null;
    for (final long currLong : longSortedSet) {
      if (anchorLong == null) {
        anchorLong = currLong;
        prevLong = currLong;
        continue;
      }

      if (currLong == prevLong + 1) {
        prevLong = currLong;
      } else {
        longRangeSet.add(new LongRange(anchorLong.longValue(), prevLong.longValue()));
        anchorLong = currLong;
        prevLong = currLong;
      }
    }

    if (anchorLong != null) {
      longRangeSet.add(new LongRange(anchorLong.longValue(), prevLong.longValue()));
    }

    return longRangeSet;
  }

  /**
   * Get the number of units inside the set of ranges.
   * @param rangeSet Set of ranges.
   * @return Number of units.
   */
  public static long getNumUnits(final Set<LongRange> rangeSet) {
    long numUnits = 0;
    for (final LongRange idRange : rangeSet) {
      numUnits += (idRange.getMaximumLong() - idRange.getMinimumLong() + 1);
    }
    return numUnits;
  }

  /**
   * @return Intersection of the two ranges. {@code null} if the ranges are disjoint.
   */
  public static LongRange getIntersection(final LongRange range1, final LongRange range2) {
    if (!range1.overlapsRange(range2)) {
      return null;
    } else {
      // The intersection is the [larger min, smaller max].
      final long min1 = range1.getMinimumLong();
      final long max1 = range1.getMaximumLong();
      final long min2 = range2.getMinimumLong();
      final long max2 = range2.getMaximumLong();

      return new LongRange(Math.max(min1, min2), Math.min(max1, max2));
    }
  }

  /**
   * @return Complement of the two ranges (range1 - range2). If the range2 is inside range1, then
   * returns the two sub-ranges which is split into two.
   */
  public static Set<LongRange> getComplement(final LongRange range1, final LongRange range2) {
    final Set<LongRange> complement = new HashSet<>();
    if (!range1.overlapsRange(range2)) {
      complement.add(range1);
      return complement;
    } else {
      final LongRange intersect = getIntersection(range1, range2);
      // If two sections are overlapping, we can divide into three parts: LEFT | CENTER | RIGHT
      // LEFT ([leftEnd (centerLeft-1)] and RIGHT ([(centerRight+1) rightEnd]) if they are not empty.
      final long min1 = range1.getMinimumLong();
      final long max1 = range1.getMaximumLong();
      final long min2 = intersect.getMinimumLong();
      final long max2 = intersect.getMaximumLong();
      final long minMax = Math.min(max1, max2);
      final long maxMin = Math.max(min1, min2);

      final long leftEnd = Math.min(min1, min2);
      final long centerLeft = Math.min(minMax, maxMin);
      final long centerRight = Math.max(minMax, maxMin);
      final long rightEnd = Math.max(max1, max2);

      // Add LEFT only if exists.
      if (leftEnd < centerLeft) {
        final LongRange left = new LongRange(leftEnd, centerLeft - 1);
        complement.add(left);
      }

      // Add RIGHT only if exists.
      if (rightEnd > centerRight) {
        final LongRange right = new LongRange(centerRight + 1, rightEnd);
        complement.add(right);
      }

      return complement;
    }
  }
}
