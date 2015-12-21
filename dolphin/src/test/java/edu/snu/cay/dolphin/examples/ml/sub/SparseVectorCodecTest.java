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

import no.uib.cipr.matrix.Vector;
import no.uib.cipr.matrix.sparse.SparseVector;
import org.apache.mahout.common.RandomUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public final class SparseVectorCodecTest {

  private SparseVectorCodec sparseVectorCodec;
  private Random random;

  @Before
  public void setUp() throws InjectionException {
    this.sparseVectorCodec = Tang.Factory.getTang().newInjector().getInstance(SparseVectorCodec.class);
    this.random = RandomUtils.getRandom();
  }

  private Vector generateSparseVector(final int cardinality, final int size) {
    final Vector ret = new SparseVector(cardinality, size);
    for (int i = 0; i < size; ++i) {
      ret.set(random.nextInt(ret.size()), random.nextGaussian());
    }
    return ret;
  }

  @Test
  public void testSparseVectorCodec() {
    final Vector sparseVectorInput = generateSparseVector(30, 4);
    final Vector sparseVectorOutput = sparseVectorCodec.decode(sparseVectorCodec.encode(sparseVectorInput));
    assertEquals(sparseVectorInput.size(), sparseVectorOutput.size());
    final Vector diff = sparseVectorInput.copy().add(-1, sparseVectorOutput);
    assertEquals(diff.norm(Vector.Norm.One), 0.0, 0.0);
  }
}
