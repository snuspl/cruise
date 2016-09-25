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
 * Tests for {@link TrainingDataProvider} provides training data instances for mini-batches correctly.
 */
public class TrainingDataProviderTest {
  private static final int MINI_BATCH_SIZE = 5;

  private final Map<Integer, Integer> values = new HashMap<>();

  private MemoryStore<Integer> mockMemoryStore;
  private TrainingDataProvider trainingDataProvider;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Parameters.MiniBatchSize.class, Integer.toString(MINI_BATCH_SIZE))
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
    trainingDataProvider = injector.getInstance(TrainingDataProvider.class);
  }

  /**
   * Test getNextTrainingData when the number of total instances is multiple of MINI_BATCH_SIZE.
   * Then the number of instances included in a batch should all be same.
   */
  @Test
  public void testDivisibleByMiniBatchSize() {
    final int numTotalInstances = MINI_BATCH_SIZE * 5;
    createMockTrainingData(numTotalInstances);

    trainingDataProvider.prepareDataForEpoch();

    assertTrue(numTotalInstances % MINI_BATCH_SIZE == 0);
    final int numTotalMiniBatches = (int) Math.ceil((double) numTotalInstances / MINI_BATCH_SIZE);
    final int numInstances = numTotalInstances / MINI_BATCH_SIZE;
    int miniBatchIdx = 0;
    Map<Integer, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      miniBatchIdx++;
      for (final Integer key : trainingData.keySet()) {
        assertEquals("The training data for a key should be same", values.get(key), trainingData.get(key));
      }
      assertEquals("TrainingDataProvider did not give the expected number of instances",
          numInstances, trainingData.size());
      trainingData = trainingDataProvider.getNextTrainingData();
    }

    assertEquals("The total number of mini-batch is different from expectation",
        numTotalMiniBatches, miniBatchIdx);
  }

  /**
   * Test getNextTrainingData when the number of total instances is indivisible by MINI_BATCH_SIZE.
   * Then the number of instances is smaller than the MINI_BATCH_SIZE in the last mini-batch.
   */
  @Test
  public void testIndivisibleMiniBatchSize() {

    // With 23 instances in total, the first 4 mini-batches process 5 instances (MINI_BATCH_SIZE),
    // and the last mini-batch processes remaining 3 instances.
    final int numTotalMiniBatches = 5;
    final int numRemainderForLastMiniBatch = (MINI_BATCH_SIZE - 2);
    final int numTotalInstances = MINI_BATCH_SIZE * (numTotalMiniBatches - 1) + numRemainderForLastMiniBatch;
    createMockTrainingData(numTotalInstances);

    trainingDataProvider.prepareDataForEpoch();

    assertTrue(numTotalInstances % MINI_BATCH_SIZE != 0);
    int miniBatchIdx = 0;
    Map<Integer, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      miniBatchIdx++;
      for (final Integer key : trainingData.keySet()) {
        assertEquals(values.get(key), trainingData.get(key));
      }
      if (miniBatchIdx < numTotalMiniBatches) {
        assertEquals("Should process MINI_BATCH_SIZE instances", MINI_BATCH_SIZE, trainingData.size());
      } else {
        assertEquals("The last mini-batch should process remaining instances",
            numRemainderForLastMiniBatch, trainingData.size());
      }
      trainingData = trainingDataProvider.getNextTrainingData();
    }
    assertEquals("The total number of mini-batches is different from expectation",
        numTotalMiniBatches, miniBatchIdx);
  }

  /**
   * Generate random values and put them into {@link #values} to be used as training data in {@link #mockMemoryStore}.
   */
  private void createMockTrainingData(final int numTotalInstances) {
    final Random generator = new Random();
    for (int i = 0; i < numTotalInstances; i++) {
      final int value = generator.nextInt();
      values.put(i, value);
    }
  }
}
