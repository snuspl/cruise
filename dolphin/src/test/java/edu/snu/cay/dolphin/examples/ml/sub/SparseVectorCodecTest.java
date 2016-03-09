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
package edu.snu.cay.dolphin.examples.ml.sub;

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.common.math.linalg.VectorFactory;
import org.apache.mahout.common.RandomUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public final class SparseVectorCodecTest {

  private SparseVectorCodec sparseVectorCodec;
  private Random random;
  private VectorFactory vectorFactory;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.sparseVectorCodec = injector.getInstance(SparseVectorCodec.class);
    this.random = RandomUtils.getRandom();
    this.vectorFactory = injector.getInstance(VectorFactory.class);
  }

  private Vector generateSparseVector(final int cardinality, final int size) {
    final Vector ret = vectorFactory.createSparseZeros(cardinality);
    for (int i = 0; i < size; ++i) {
      ret.set(random.nextInt(ret.length()), random.nextGaussian());
    }
    return ret;
  }

  @Test
  public void testSparseVectorCodec() {
    final Vector sparseVectorInput = generateSparseVector(30, 4);
    assertEquals(sparseVectorInput, sparseVectorCodec.decode(sparseVectorCodec.encode(sparseVectorInput)));
  }
}
