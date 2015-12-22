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

import edu.snu.cay.dolphin.breeze.Vector;
import edu.snu.cay.dolphin.breeze.VectorFactory;
import edu.snu.cay.dolphin.examples.ml.data.Row;
import org.apache.mahout.common.RandomUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Test Codec for row with a sparse vector.
 */
public final class SparseRowCodecTest {

  private SparseRowCodec sparseRowCodec;
  private Random random;
  private VectorFactory vectorFactory;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.sparseRowCodec = injector.getInstance(SparseRowCodec.class);
    this.random = RandomUtils.getRandom();
    this.vectorFactory = injector.getInstance(VectorFactory.class);
  }

  private Row generateSparseRow(final int cardinality, final int size) {
    final double output = random.nextDouble();

    final Vector feature = vectorFactory.newSparseVector(cardinality);
    for (int i = 0; i < size; ++i) {
      feature.set(random.nextInt(feature.length()), random.nextGaussian());
    }

    return new Row(output, feature);
  }

  @Test
  public void testSparseRowCodec() {
    final Row row = generateSparseRow(30, 4);
    assertEquals(row, sparseRowCodec.decode(sparseRowCodec.encode(row)));
  }
}
