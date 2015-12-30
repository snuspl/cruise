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
package edu.snu.cay.common.math.vector.breeze;

import edu.snu.cay.common.math.vector.Vector;
import edu.snu.cay.common.math.vector.VectorFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This tests vector operations.
 */
public final class VectorOpsTest {

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
   * Tests dense vector operations work well as intended.
   */
  @Test
  public void testDenseVector() {
    final double[] value1 = {0.1, 0.2, 0.3, 0.4, 0.5};
    final double[] value2 = {0.1, 0.3, 0.5, 0.7, 0.9};

    final Vector vec1 = factory.newDenseVector(value1);
    final Vector vec2 = factory.newDenseVector(value2);

    double dotResult = 0;
    for (int i = 0; i < vec1.length(); i++) {
      dotResult += vec1.get(i) * vec2.get(i);
    }
    assertEquals(dotResult, vec1.dot(vec2), 0.0);

    final Vector addVec = vec1.add(vec2);
    vec1.addi(vec2);
    assertEquals(addVec, vec1);

    final Vector scaleVec = vec1.scale(1.5);
    vec1.scalei(1.5);
    assertEquals(scaleVec, vec1);

    final Vector addScaleVec = vec1.add(vec2);
    vec1.axpy(1.0, vec2);
    assertEquals(addScaleVec, vec1);
  }

  /**
   * Tests sparse vector operations work well as intended.
   */
  @Test
  public void testSparseVector() {
    final int[] index1 = {0, 1, 2, 3, 4};
    final double[] value1 = {0.1, 0.2, 0.3, 0.4, 0.5};
    final int[] index2 = {0, 2, 4, 6, 8};
    final double[] value2 = {0.1, 0.3, 0.5, 0.7, 0.9};

    final Vector vec1 = factory.newSparseVector(index1, value1, 10);
    final Vector vec2 = factory.newSparseVector(index2, value2, 10);
    final Vector dVec = factory.newDenseVector(10);
    for (int i = 0; i < value1.length; i++) {
      dVec.set(i, value1[i]);
    }

    double dotResult = 0;
    for (int i = 0; i < vec1.length(); i++) {
      dotResult += vec1.get(i) * vec2.get(i);
    }
    assertEquals(dotResult, vec1.dot(vec2), 0.0);

    final Vector addVec = vec1.add(vec2);
    dVec.addi(vec2);
    assertEquals(addVec, dVec);

    assertEquals(addVec.scale(2.0), dVec.scale(2.0));

    vec1.axpy(1.0, vec2);
    assertEquals(dVec, vec1);
  }
}
