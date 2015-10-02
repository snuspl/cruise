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
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

/**
 * Testing for the LongRange operations.
 */
public final class LongRangeUtilsTest {
  private static final LongRange ORIGINAL = new LongRange(2, 7);
  private static final LongRange OUTSIDE = new LongRange(0, 1);
  private static final LongRange SAME_EDGE_OUTSIDE = new LongRange(1, 2);
  private static final LongRange SAME_EDGE_INSIDE = new LongRange(2, 3);
  private static final LongRange INSIDE = new LongRange(4, 5);

  @Before
  public void setUp() {
  }

  /**
   * Testing for verifying the intersection is computed correctly.
   */
  @Test
  public void testIntersection() {
    // [2, 7] intersection [0, 1] = null
    final LongRange intersection1 = LongRangeUtils.getIntersection(ORIGINAL, OUTSIDE);
    // The result should be null
    assertNull(intersection1);

    // [2, 7] intersection [2, 3] = [2, 3]
    final LongRange intersection2 = LongRangeUtils.getIntersection(ORIGINAL, SAME_EDGE_INSIDE);
    assertEquals(SAME_EDGE_INSIDE, intersection2);

    // [2, 7] intersection [1, 2] = [2, 2]
    final LongRange intersection3 = LongRangeUtils.getIntersection(ORIGINAL, SAME_EDGE_OUTSIDE);
    assertEquals(new LongRange(2, 2), intersection3);

    // [2, 7] intersection [4, 5] = [4, 5]
    final LongRange intersection4 = LongRangeUtils.getIntersection(ORIGINAL, INSIDE);
    assertEquals(new LongRange(4, 5), intersection4);
  }

  /**
   * Testing for verifying the complement is computed correctly.
   */
  @Test
  public void testComplement() {
    // [2, 7] - [0, 1] = [2, 7]
    final Set<LongRange> complement1 = LongRangeUtils.getComplement(ORIGINAL, OUTSIDE);
    // The result should be ORIGINAL, and should not contain [0, 1]
    assertEquals(1, complement1.size());
    assertTrue(exists(ORIGINAL, complement1));
    assertFalse(exists(OUTSIDE, complement1));

    // [2, 7] - [2, 3] = [4, 7]
    final Set<LongRange> complement2 = LongRangeUtils.getComplement(ORIGINAL, SAME_EDGE_INSIDE);
    // The result should only consist of [4, 7].
    assertEquals(1, complement2.size());
    assertTrue(exists(new LongRange(4, 7), complement2));
    assertFalse(exists(SAME_EDGE_INSIDE, complement2));

    // [2, 7] - [1, 2] = [3, 7]
    final Set<LongRange> complement3 = LongRangeUtils.getComplement(ORIGINAL, SAME_EDGE_OUTSIDE);
    // The result should only consist of [3, 7].
    assertEquals(1, complement3.size());
    assertTrue(exists(new LongRange(3, 7), complement3));
    assertFalse(exists(SAME_EDGE_OUTSIDE, complement3));

    // [2, 7] - [4, 5] = [2, 3], [6, 7]
    final Set<LongRange> complement4 = LongRangeUtils.getComplement(ORIGINAL, INSIDE);
    assertEquals(2, complement4.size());
    // The result should consist of [2, 3], [6, 7]
    assertTrue(exists(new LongRange(2, 3), complement4));
    assertTrue(exists(new LongRange(6, 7), complement4));
    assertFalse(exists(INSIDE, complement4));
  }

  /**
   * Helper method that checks whether the range is in the scope.
   * Note that scope.contains(toCheck) does not guarantee the exact membership.
   * You can see how the Comparator is defined in {@link LongRangeUtils#createEmptyTreeSet()}.
   * @return {@code true} if the scope contains the exact same range as toCheck.
   */
  private boolean exists(final LongRange toCheck, final Set<LongRange> scope) {
    for (final LongRange range : scope) {
      if (range.equals(toCheck)) {
        return true;
      }
    }
    return false;
  }
}
