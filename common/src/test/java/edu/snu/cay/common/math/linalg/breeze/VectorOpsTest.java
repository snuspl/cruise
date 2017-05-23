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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This tests vector operations.
 */
public final class VectorOpsTest {

  private static final double EPSILON = 0.01;
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
    final Float[] value1 = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
    final Float[] value2 = {0.1f, 0.3f, 0.5f, 0.7f, 0.9f};

    final Vector vec1 = factory.createDense(value1);
    final Vector vec2 = factory.createDense(value2);
    final Vector vec3 = factory.createDenseOnes(5);
    final Vector vec4 = factory.createDenseZeros(5);

    double dotResult = 0;
    for (int i = 0; i < vec1.length(); i++) {
      dotResult += vec1.get(i) * vec2.get(i);
    }
    assertEquals(dotResult, vec1.dot(vec2), 0.0);

    final Vector addVec = vec1.add(vec2);
    vec1.addi(vec2);
    assertEquals(addVec, vec1);

    final Vector scaleVec = vec1.scale(1.5f);
    vec1.scalei(1.5f);
    assertEquals(scaleVec, vec1);

    final Vector addScaleVec = vec1.add(vec2);
    vec1.axpy(1.0f, vec2);
    assertEquals(addScaleVec, vec1);

    assertEquals(vec3, vec4.add(1));
    vec4.subi(1);
    vec3.addi(-2);
    assertEquals(vec3, vec4);

    final Vector sliceVec = vec1.slice(2, 4);
    assertEquals(2, sliceVec.length());
    assertEquals(vec1.get(2), sliceVec.get(0), EPSILON);
    assertEquals(vec1.get(3), sliceVec.get(1), EPSILON);

    // check that changes on sliceVec affect the original vector
    sliceVec.set(0, 1.0f);
    assertEquals(vec1.get(2), sliceVec.get(0), EPSILON);
    assertEquals(vec1.get(3), sliceVec.get(1), EPSILON);
  }

  /**
   * Tests sparse vector operations work well as intended.
   */
  @Test
  public void testSparseVector() {
    final int[] index1 = {0, 1, 2, 3, 4};
    final Float[] value1 = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
    final int[] index2 = {0, 2, 4, 6, 8};
    final Float[] value2 = {0.1f, 0.3f, 0.5f, 0.7f, 0.9f};

    final Vector vec1 = factory.createSparse(index1, value1, 10);
    final Vector vec2 = factory.createSparse(index2, value2, 10);
    final Vector vec3 = factory.createSparseZeros(10);
    final Vector vec4 = factory.createSparseZeros(10);
    final Vector dVec = factory.createDenseZeros(10);
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

    assertEquals(addVec.scale(2.0f), dVec.scale(2.0f));

    vec1.axpy(1.0f, vec2);
    assertEquals(dVec, vec1);

    vec3.addi(1);
    assertEquals(vec3, vec4.add(1));
    vec4.subi(1);
    assertEquals(vec3.sub(2), vec4);
  }
}
