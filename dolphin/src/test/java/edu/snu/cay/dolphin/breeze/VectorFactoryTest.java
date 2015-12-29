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
package edu.snu.cay.dolphin.breeze;

import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This tests {@link VectorFactory}.
 */
public final class VectorFactoryTest {

  private VectorFactory factory;

  @Before
  public void setUp() {
    try {
      factory = Tang.Factory.getTang().newInjector().getInstance(VectorFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests {@link VectorFactory} creates {@link DenseVector} as intended.
   */
  @Test
  public void testDenseVector() {
    final double[] value = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    final Vector vec1 = factory.newDenseVector(10);
    final Vector vec2 = factory.newDenseVector(value);

    assertEquals(vec1.length(), 10);
    assertEquals(vec2.length(), 10);

    for (int i = 0; i < 10; i++) {
      assertEquals(vec1.get(i), 0.0, 0.0);
      assertEquals(vec2.get(i), value[i], 0.0);
    }
  }

  /**
   * Tests {@link VectorFactory} creates {@link SparseVector} as intended.
   */
  @Test
  public void testSparseVector() {
    final int[] index = {1, 3, 5, 7};
    final double[] value = {0.1, 0.2, 0.3, 0.4};
    final Vector vec1 = factory.newSparseVector(10);
    final Vector vec2 = factory.newSparseVector(index, value, 10);

    assertEquals(vec1.length(), 10);
    assertEquals(vec1.activeSize(), 0);
    assertEquals(vec2.length(), 10);
    assertEquals(vec2.activeSize(), 4);

    int i = 0;
    for (final VectorEntry element : vec2) {
      assertEquals(element.index(), index[i]);
      assertEquals(element.value(), value[i], 0.0);
      i++;
    }
  }
}
