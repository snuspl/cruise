/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link TrainingDataSplitter} splits data correctly.
 */
public class TrainingDataSplitterTest {
  private static final int MINI_BATCHES_PER_EPOCH = 5;

  private final Map<Integer, Integer> values = new HashMap<>();

  private MemoryStore<Integer> mockMemoryStore;
  private TrainingDataSplitter trainingDataSplitter;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Parameters.MiniBatches.class, Integer.toString(MINI_BATCHES_PER_EPOCH))
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    mockMemoryStore = mock(MemoryStore.class);
    injector.bindVolatileInstance(MemoryStore.class, mockMemoryStore);
    doAnswer(invocation -> values).when(mockMemoryStore).getAll();
    doAnswer(invocation -> {
        final Integer id = invocation.getArgumentAt(0, Integer.class);
        final Pair<Integer, Integer> pair = new Pair<>(id, values.get(id));
        return pair;
      }).when(mockMemoryStore).get(anyObject());
    trainingDataSplitter = injector.getInstance(TrainingDataSplitter.class);
  }

  /**
   * Test getNextTrainingDataSplit when data size can be divided by MINI_BATCHES_PER_EPOCH.
   * It means sizes of all mini batch splits are same.
   */
  @Test
  public void testGetNextTrainingDataSplit1() {
    final Random generator = new Random();

    final int dataCount1 = 10;
    for (int i = 0; i < dataCount1; i++) {
      final int value  = generator.nextInt();
      values.put(i, value);
    }
    trainingDataSplitter.prepareSplitsForEpoch();

    assertTrue(dataCount1 % MINI_BATCHES_PER_EPOCH == 0);
    final int dataSplitSize1 = dataCount1 / MINI_BATCHES_PER_EPOCH;
    int dataSplitCount = 0;
    Map<Integer, Integer> dataSplit = trainingDataSplitter.getNextTrainingDataSplit();
    while (!dataSplit.isEmpty()) {
      dataSplitCount++;
      for (final Integer key : dataSplit.keySet()) {
        assertEquals(values.get(key), dataSplit.get(key));
      }
      assertEquals(dataSplitSize1, dataSplit.size());
      dataSplit = trainingDataSplitter.getNextTrainingDataSplit();
    }
    // test whether TrainingDataSplitter splits the data by MINI_BATCHES_PER_EPOCH
    assertEquals(MINI_BATCHES_PER_EPOCH, dataSplitCount);
  }

  /**
   * Test getNextTrainingDataSplit when data size cannot be divided by MINI_BATCHES_PER_EPOCH.
   * It means sizes of mini batch splits are vary.
   */
  @Test
  public void testGetNextTrainingDataSplit2() {
    final Random generator = new Random();

    final int dataCount2 = 32;
    for (int i = 0; i < dataCount2; i++) {
      final int value = generator.nextInt();
      values.put(i, value);
    }
    trainingDataSplitter.prepareSplitsForEpoch();

    assertTrue(dataCount2 % MINI_BATCHES_PER_EPOCH != 0);
    final int dataSplitSize2 = dataCount2 / MINI_BATCHES_PER_EPOCH;
    int dataSplitCount = 0;
    Map<Integer, Integer> dataSplit = trainingDataSplitter.getNextTrainingDataSplit();
    while (!dataSplit.isEmpty()) {
      dataSplitCount++;
      for (final Integer key : dataSplit.keySet()) {
        assertEquals(values.get(key), dataSplit.get(key));
      }
      if (dataSplitCount <= dataCount2 % MINI_BATCHES_PER_EPOCH) {
        assertEquals(dataSplitSize2 + 1, dataSplit.size());
      } else {
        // the last split is bigger than previous splits
        assertEquals(dataSplitSize2, dataSplit.size());
      }
      dataSplit = trainingDataSplitter.getNextTrainingDataSplit();
    }
    // test whether TrainingDataSplitter splits the data by MINI_BATCHES_PER_EPOCH
    assertEquals(MINI_BATCHES_PER_EPOCH, dataSplitCount);
  }
}
