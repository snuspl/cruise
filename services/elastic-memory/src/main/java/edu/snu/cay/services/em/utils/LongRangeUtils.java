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
package edu.snu.cay.services.em.utils;

import org.apache.commons.lang.math.LongRange;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

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
}
