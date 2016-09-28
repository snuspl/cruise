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
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
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
  private static final int NUM_MEMORY_STORES = 2;
  private static final int LOCAL_STORE_ID = 0;
  private static final int REMOTE_STORE_ID = 1;
  private static final int MIN_BLOCK_ID = 0;

  private final Map<Long, Integer> kvMapBackingMemoryStore = new HashMap<>();

  private MemoryStore<Long> memoryStore;
  private OperationRouter<Long> operationRouter;
  private TrainingDataProvider<Long> trainingDataProvider;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(LOCAL_STORE_ID))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(NUM_MEMORY_STORES))
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
    operationRouter = injector.getInstance(OperationRouter.class);
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
  
  @Test
  public void testApplyBlockAdditionsToTrainingData() {
    final int numTotalInstances = MINI_BATCH_SIZE * 10;

    createTrainingData(numTotalInstances);
    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingDataWithBlockAdditions(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }

  private List<Pair<Integer, Map<Long, Integer>>> testGetNextTrainingDataWithBlockAdditions(
      final int initialNumTotalInstances) {
    final List <Integer> blockIdList = generateBlockIdNotInLocalStore();
    final Random randomNumberGenerator = new Random(System.currentTimeMillis());
    final MoveHandler moveHandler = (MoveHandler) memoryStore;
    final List<Pair<Integer, Map<Long, Integer>>> blockDataPairList = new ArrayList<>();

    int numTotalInstances = initialNumTotalInstances;
    int numUsedInstances = 0;
    Map<Long, Integer> trainingData = trainingDataProvider.getNextTrainingData();

    while (!trainingData.isEmpty()) {
      // add a new block to the MemoryStore in probability.
      if ((randomNumberGenerator.nextInt() % 7 == 0) && (blockIdList.size() > 0)) {
        final int blockId = blockIdList.remove(0).intValue();
        final Map<Long, Integer> dataSet = generateDataSetForBlock(blockId, MAX_BLOCK_SIZE);

        // add a new block to the MemoryStore
        moveHandler.putBlock(blockId, (Map) dataSet);
        operationRouter.updateOwnership(blockId, REMOTE_STORE_ID, LOCAL_STORE_ID);

        // add the data set of the new block to the expected data set
        kvMapBackingMemoryStore.putAll(dataSet);
        blockDataPairList.add(new Pair(blockId, dataSet));

        // increase the number of total instances by the size of new data set
        numTotalInstances += dataSet.size();
      }

      // compare the training data with the expected data set
      final Set<Long> keySet = trainingData.keySet();
      for (final Long key : keySet) {
        assertEquals(kvMapBackingMemoryStore.get(key), trainingData.get(key));
      }

      // update the number of used instances by the size of training data
      numUsedInstances += trainingData.size();
      trainingData = trainingDataProvider.getNextTrainingData();
    }

    assertEquals(numTotalInstances, numUsedInstances);
    return blockDataPairList;
  }

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

  private List<Integer> generateBlockIdNotInLocalStore() {
    final List<Integer> blockIdsInLocalStore = operationRouter.getCurrentLocalBlockIds();
    final List<Integer> blockIdList = new ArrayList<>();
    final int minBlockId = MIN_BLOCK_ID;
    final int maxBlockId = MIN_BLOCK_ID + NUM_TOTAL_BLOCKS;
    for (int blockId = minBlockId; blockId < maxBlockId; blockId++) {
      if (blockIdList.contains(blockId) || blockIdsInLocalStore.contains(blockId)) {
        continue;
      }
      blockIdList.add(blockId);
    }
    return blockIdList;
  }
}
