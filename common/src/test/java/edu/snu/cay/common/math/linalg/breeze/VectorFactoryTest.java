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
package edu.snu.cay.common.math.linalg.breeze;

import com.google.common.collect.Lists;
import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorEntry;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * This tests {@link DefaultVectorFactory}.
 */
public final class VectorFactoryTest {

  private static final double EPSILON = 0.01;
  private VectorFactory factory;

  @Before
  public void setUp() {
    try {
      factory = Tang.Factory.getTang().newInjector().getInstance(DefaultVectorFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Tests {@link DefaultVectorFactory} creates {@link DenseVector} as intended.
   */
  @Test
  public void testDenseVector() {
    final double[] value = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    final Vector vec1 = factory.createDenseZeros(10);
    final Vector vec2 = factory.createDense(value);
    final Vector vec3 = factory.createDenseOnes(10);

    assertEquals(vec1.length(), 10);
    assertEquals(vec2.length(), 10);

    for (int i = 0; i < 10; i++) {
      assertEquals(vec1.get(i), 0.0, 0.0);
      assertEquals(vec2.get(i), value[i], 0.0);
    }
    assertEquals(vec2, vec3);

    final Vector vec4 = factory.concatDense(Lists.newArrayList(vec1, vec2));
    for (int i = 0; i < 10; i++) {
      assertEquals(vec1.get(i), vec4.get(i), EPSILON);
      assertEquals(vec2.get(i), vec4.get(i + 10), EPSILON);
    }

    vec4.set(1, 10);
    assertNotEquals(vec1.get(1), vec4.get(1), EPSILON);
  }

  /**
   * Tests {@link DefaultVectorFactory} creates {@link SparseVector} as intended.
   */
  @Test
  public void testSparseVector() {
    final int[] index = {1, 3, 5, 7};
    final double[] value = {0.1, 0.2, 0.3, 0.4};
    final Vector vec1 = factory.createSparseZeros(10);
    final Vector vec2 = factory.createSparse(index, value, 10);

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
