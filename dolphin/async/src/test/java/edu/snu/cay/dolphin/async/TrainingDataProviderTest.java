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
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.evaluator.api.BlockResolver;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
//import edu.snu.cay.services.em.evaluator.api.MoveHandler;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.services.em.evaluator.impl.rangekey.MemoryStoreImpl;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.commons.lang.NotImplementedException;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link TrainingDataProvider} provides training data instances for mini-batches correctly.
 */
public class TrainingDataProviderTest {
  private static final int MINI_BATCH_SIZE = 5;
  private static final int NUM_TOTAL_BLOCKS = 0x100;
  private static final int MAX_BLOCK_SIZE = 128;

  private final Map<Long, Integer> kvMapBackingMemoryStore = new HashMap<>();

  private MemoryStore<Long> memoryStore;
  private TrainingDataProvider<Long> trainingDataProvider;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(0))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(1))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .bindNamedParameter(Parameters.MiniBatchSize.class, Integer.toString(MINI_BATCH_SIZE))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
    injector.bindVolatileInstance(ElasticMemoryMsgSender.class, mock(ElasticMemoryMsgSender.class));
    injector.bindVolatileInstance(RemoteAccessibleMemoryStore.class, mock(RemoteAccessibleMemoryStore.class));

    final BlockResolver<Long> mockBlockResolver = mock(BlockResolver.class);
    injector.bindVolatileInstance(BlockResolver.class, mockBlockResolver);
    doAnswer(invocation -> {
        throw new NotImplementedException();
      }).when(mockBlockResolver).resolveBlock(anyLong());

    doAnswer(invocation -> {
        final Long minKey = (Long) invocation.getArguments()[0];
        final Long maxKey = (Long) invocation.getArguments()[1];
        final Map<Integer, Pair<Long, Long>> blockToKeyRange = new HashMap<>();

        assertEquals(minKey, maxKey);
        blockToKeyRange.put((minKey.intValue() / MAX_BLOCK_SIZE), new Pair<>(minKey, maxKey));

        return blockToKeyRange;
      }).when(mockBlockResolver).resolveBlocksForOrderedKeys(anyLong(), anyLong());

    memoryStore = injector.getInstance(MemoryStore.class);
    trainingDataProvider = injector.getInstance(TrainingDataProvider.class);
  }

  /**
   * Test getNextTrainingData when the number of total instances is multiple of MINI_BATCH_SIZE.
   * Then the number of instances included in a batch should all be same.
   */
  @Test
  public void testDivisibleByMiniBatchSize() {
    final int numTotalInstances = MINI_BATCH_SIZE * 5;
    createTrainingData(numTotalInstances);

    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());

    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }

  /**
   * Test getNextTrainingData when the number of total instances is indivisible by MINI_BATCH_SIZE.
   * Then the number of instances is smaller than the MINI_BATCH_SIZE in the last mini-batch.
   */
  @Test
  public void testIndivisibleMiniBatchSize() {
    // With 23 instances in total, the first 4 mini-batches process 5 instances (MINI_BATCH_SIZE),
    // and the last mini-batch processes remaining 3 instances.
    final int numTotalInstances = 23;
    createTrainingData(numTotalInstances);

    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());

    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }

  /**
   * Generate random values and put them into {@link #kvMapBackingMemoryStore}
   * to be used as training data in {@link #memoryStore}.
   */
  private void createTrainingData(final int numTotalInstances) {
    kvMapBackingMemoryStore.clear();
    final Random generator = new Random();
    for (int i = 0; i < numTotalInstances; i++) {
      final int value = generator.nextInt();
      memoryStore.put((long)i, value);
    }

    kvMapBackingMemoryStore.putAll(memoryStore.getAll());
  }

  private void testGetNextTrainingData(final int numTotalInstances) {
    final int remainderForLatchMiniBatch = numTotalInstances % MINI_BATCH_SIZE;
    final int numMiniBatches = numTotalInstances / MINI_BATCH_SIZE + (remainderForLatchMiniBatch == 0 ? 0 : 1);
    final int sizeOfLastMiniBatch = remainderForLatchMiniBatch == 0 ? MINI_BATCH_SIZE : remainderForLatchMiniBatch;

    int miniBatchIdx = 0;
    Map<Long, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      miniBatchIdx++;
      for (final Long key : trainingData.keySet()) {
        assertEquals(kvMapBackingMemoryStore.get(key), trainingData.get(key));
      }

      if (miniBatchIdx < numMiniBatches) {
        assertEquals("Should process MINI_BATCH_SIZE instances", MINI_BATCH_SIZE, trainingData.size());
      } else if (miniBatchIdx == numMiniBatches) {
        assertEquals("The last mini-batch should process remaining instances",
            sizeOfLastMiniBatch, trainingData.size());
      } else {
        fail("The total number of mini-batches is larger than expectation");
      }

      trainingData = trainingDataProvider.getNextTrainingData();
    }
    assertEquals("The total number of mini-batches is different from expectation",
        numMiniBatches, miniBatchIdx);
  }

  /*
  private Map<Long, Integer> generateDataSetForBlock(final int blockId, final int dataSetSize) {
    final Random generator = new Random();
    final Map<Long, Integer> data = new HashMap<>();
    for (int i = 0; i < dataSetSize; i++) {
      final int key = blockId * dataSetSize + i;
      final int value = generator.nextInt();
      data.put((long)key, value);
    }
    return data;
  }

  @Test
  public void testApplyBlockChangesToTrainingData() {
    final int numTotalInstances = MINI_BATCH_SIZE * 5;
    // assign a new block id to a block to be added/removed.
    // the value 0x40 is used as a new block id,
    // to avoid a conflict with ids already assigned to initial blocks in the MemoryStore.
    final int blockId = 0x40;
    final MoveHandler<Long> moveHandler = (MoveHandler) memoryStore;
    Map<Long, Integer> dataSet;

    //createTrainingData(numTotalInstances);
    trainingDataProvider.prepareDataForEpoch();

    // add a new block to the MemoryStore.
    dataSet = generateDataSetForBlock(blockId, MAX_BLOCK_SIZE);
    moveHandler.putBlock(blockId, (Map) dataSet);
    kvMapBackingMemoryStore.putAll(dataSet);

    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());

    // remove the block from the MemoryStore.
    trainingDataProvider.prepareDataForEpoch();
    moveHandler.removeBlock(blockId);
    for (final Long key : dataSet.keySet()) {
      kvMapBackingMemoryStore.remove(key);
    }
    testGetNextTrainingData(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }
  */
}
