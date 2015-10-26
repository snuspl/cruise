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
package edu.snu.cay.dophin.examples.ml.sub;

import edu.snu.cay.dolphin.examples.ml.sub.IntegerListCodec;
import org.apache.mahout.common.RandomUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public final class IntegerListCodecTest {

  private IntegerListCodec integerListCodec;
  private Random random;

  @Before
  public void setUp() throws InjectionException {
    this.integerListCodec = Tang.Factory.getTang().newInjector().getInstance(IntegerListCodec.class);
    this.random = RandomUtils.getRandom();
  }

  private List<Integer> generateIntegerList(final int size) {
    final List<Integer> integerList = new ArrayList<Integer>(size);
    for (int i = 0; i < size; i++) {
      integerList.add(random.nextInt());
    }
    return integerList;
  }

  @Test
  public void testIntegerListCodec() {
    final List<Integer> integerList = generateIntegerList(50);
    assertEquals(integerList, integerListCodec.decode(integerListCodec.encode(integerList)));
  }
}
