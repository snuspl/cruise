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

import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.evaluator.api.*;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.services.em.evaluator.impl.rangekey.BlockFactoryImpl;
import edu.snu.cay.services.em.evaluator.impl.rangekey.MemoryStoreImpl;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
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

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link EMTrainingDataProvider} provides training data instances for mini-batches correctly.
 */
public class EMTrainingDataProviderTest {
  private static final int MINI_BATCH_SIZE = 5;
  private static final int NUM_TOTAL_BLOCKS = 0x10;
  private static final int BLOCK_SIZE = 128;
  private static final int NUM_MEMORY_STORES = 2;
  private static final int LOCAL_STORE_ID = 0;
  private static final int REMOTE_STORE_ID = 1;
  private static final int MIN_BLOCK_ID = 0;

  private MemoryStore<Long> memoryStore;
  private OwnershipCache ownershipCache;
  private TrainingDataProvider<Long, Integer> trainingDataProvider;
  private BlockHandler<Long, Integer> blockHandler;

  @Before
  public void setup() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindImplementation(BlockFactory.class, BlockFactoryImpl.class)
        .bindImplementation(TrainingDataProvider.class, EMTrainingDataProvider.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(LOCAL_STORE_ID))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(NUM_MEMORY_STORES))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .bindNamedParameter(DolphinParameters.MiniBatchSize.class, Integer.toString(MINI_BATCH_SIZE))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(SpanReceiver.class, mock(SpanReceiver.class));
    injector.bindVolatileInstance(EMMsgSender.class, mock(EMMsgSender.class));

    final BlockResolver<Long> mockBlockResolver = mock(BlockResolver.class);
    injector.bindVolatileInstance(BlockResolver.class, mockBlockResolver);
    doAnswer(invocation -> {
      throw new NotImplementedException();
    }).when(mockBlockResolver).resolveBlock(anyLong());

    // the redefinition of resolveBlocksForOrderedKeys below works only when minKey == maxKey
    // in other words, it works only when searching for 1 key, not a range of keys.
    doAnswer(invocation -> {
      final Long minKey = (Long) invocation.getArguments()[0];
      final Long maxKey = (Long) invocation.getArguments()[1];
      final Map<Integer, Pair<Long, Long>> blockToKeyRange = new HashMap<>();

      assertEquals(minKey, maxKey);
      blockToKeyRange.put((minKey.intValue() / BLOCK_SIZE), new Pair<>(minKey, maxKey));

      return blockToKeyRange;
    }).when(mockBlockResolver).resolveBlocksForOrderedKeys(anyLong(), anyLong());

    memoryStore = injector.getInstance(MemoryStore.class);
    ownershipCache = injector.getInstance(OwnershipCache.class);
    trainingDataProvider = injector.getInstance(TrainingDataProvider.class);
    blockHandler = injector.getInstance(BlockHandler.class);
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
   * Generate random values to be used as training data in {@link #memoryStore}.
   */
  private void createTrainingData(final int numTotalInstances) {

    final Random generator = new Random();
    for (int i = 0; i < numTotalInstances; i++) {
      final int value = generator.nextInt();
      memoryStore.put((long)i, value);
    }
  }

  /**
   * Test {@link TrainingDataProvider#getNextTrainingData()} gives right size of training data for each mini-batch
   * and provides training data for the exact number of mini-batches, total number of instances / mini-batch size
   * (or +1 depending on whether total number of instances is divisible by mini-batch size or not).
   * @param numTotalInstances the number of instances {@link TrainingDataProvider} currently has.
   */
  private void testGetNextTrainingData(final int numTotalInstances) {
    final int remainderForLastMiniBatch = numTotalInstances % MINI_BATCH_SIZE;
    final int numMiniBatches = numTotalInstances / MINI_BATCH_SIZE + (remainderForLastMiniBatch == 0 ? 0 : 1);
    final int lastMiniBatchIdx = numMiniBatches - 1;
    final int sizeOfLastMiniBatch = remainderForLastMiniBatch == 0 ? MINI_BATCH_SIZE : remainderForLastMiniBatch;

    int miniBatchIdx = 0;

    Map<Long, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      for (final Long key : trainingData.keySet()) {
        assertEquals(memoryStore.get(key).getSecond(), trainingData.get(key));
      }

      if (miniBatchIdx < lastMiniBatchIdx) {
        assertEquals("Should process MINI_BATCH_SIZE instances", MINI_BATCH_SIZE, trainingData.size());
      } else if (miniBatchIdx == lastMiniBatchIdx) {
        assertEquals("The last mini-batch should process remaining instances",
            sizeOfLastMiniBatch, trainingData.size());
      } else {
        fail("The total number of mini-batches is larger than expectation");
      }

      trainingData = trainingDataProvider.getNextTrainingData();
      miniBatchIdx++;
    }
    assertEquals("The total number of mini-batches is different from expectation",
        numMiniBatches, miniBatchIdx);
  }

  /**
   * Add a block to the MemoryStore.
   * @param blockId the id of the block to be added
   * @param dataSet the data set which will be stored into the added block
   */
  private void putBlockToMemoryStore(final int blockId, final Map<Long, Integer> dataSet) {
    ownershipCache.updateOwnership(blockId, REMOTE_STORE_ID, LOCAL_STORE_ID);
    blockHandler.putBlock(blockId, dataSet);
    ownershipCache.allowAccessToBlock(blockId);
  }

  /**
   * Remove a block from the MemoryStore.
   * @param blockId the id of the block to be removed
   */
  private void removeBlockFromMemoryStore(final int blockId) {
    ownershipCache.updateOwnership(blockId, LOCAL_STORE_ID, REMOTE_STORE_ID);
    blockHandler.removeBlock(blockId);
  }

  /**
   * Add and remove a block to/from the {@link MemoryStore} at the beginning of epochs.
   * Check the block changes in the {@link MemoryStore} are immediately applied to {@link TrainingDataProvider}.
   */
  @Test
  public void testApplyBlockUpdatesToTrainingDataWhenEpochBegins() {
    final int numMiniBatches = 10;
    final int numInitialInstances = MINI_BATCH_SIZE * numMiniBatches;
    final int blockId = generateBlockIdNotInLocalStore().get(0);
    final Map<Long, Integer> dataSet = generateDataSetForBlock(blockId, BLOCK_SIZE);

    createTrainingData(numInitialInstances);

    // add a new block to TrainingDataProvider at the beginning of epoch
    trainingDataProvider.prepareDataForEpoch(); // prepare training data for the epoch
    putBlockToMemoryStore(blockId, dataSet); // add a block to the MemoryStore
    testGetNextTrainingData(numInitialInstances + dataSet.size()); // verify the result of block addition
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());

    // remove the added block from TrainingDataProvider at the beginning of epoch
    trainingDataProvider.prepareDataForEpoch(); // preparing training data for the epoch
    removeBlockFromMemoryStore(blockId); // remove a block from the MemoryStore
    testGetNextTrainingData(numInitialInstances); // verify the result of block removal
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }

  /**
   * Add and remove blocks to/from the {@link MemoryStore} during epochs.
   * Check the block changes in the {@link MemoryStore} are immediately applied to {@link TrainingDataProvider}.
   */
  @Test
  public void testApplyBlockUpdatesToTrainingDataDuringEpoch() {
    final int numMiniBatches = 10;
    final int numTotalInstances = MINI_BATCH_SIZE * numMiniBatches;

    createTrainingData(numTotalInstances);

    // add several blocks to the MemoryStore during an epoch
    trainingDataProvider.prepareDataForEpoch();
    final List <Pair<Integer, Map<Long, Integer>>> addedBlockList =
        testGetNextTrainingDataWithBlockAdditions(numTotalInstances);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());

    // remove the added blocks from the MemoryStore during an epoch
    trainingDataProvider.prepareDataForEpoch();
    testGetNextTrainingDataWithBlockRemovals(addedBlockList);
    assertTrue("Data should be exhausted", trainingDataProvider.getNextTrainingData().isEmpty());
  }

  /**
   * Test whether {@link TrainingDataProvider} provides the training data properly
   * when block removal events occur at a fixed interval during an epoch.
   * @param blocksToRemove a set of blocks to be removed stored in {@link MemoryStore}
   */
  private void testGetNextTrainingDataWithBlockRemovals(final List<Pair<Integer, Map<Long, Integer>>> blocksToRemove) {
    final int numBlocksToRemove = blocksToRemove.size();
    final Map<Long, Integer> unusedDataInStore = new HashMap<>();

    // initialize training data currently remained in the MemoryStore
    unusedDataInStore.putAll(memoryStore.getAll());

    final int numTotalInstances = unusedDataInStore.size();

    int miniBatchIdx = 0;
    int removedBlockCount = 0;
    int numRemovedInstances = 0;
    int numUsedInstances = 0;

    Map<Long, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      // compare the given training data with the expected data set
      final Set<Long> keySet = trainingData.keySet();
      numUsedInstances += keySet.size();
      for (final Long key : keySet) {
        final Integer expectedValue = unusedDataInStore.remove(key);
        final Integer actualValue = trainingData.get(key);
        assertNotNull(expectedValue);
        assertEquals(expectedValue, actualValue);
      }

      // remove a new block to the MemoryStore every fourth mini-batch including 0th
      if (((miniBatchIdx % 4) == 0) && (removedBlockCount < numBlocksToRemove)) {
        final Pair<Integer, Map<Long, Integer>> blockDataPair = blocksToRemove.get(removedBlockCount);
        final int blockId = blockDataPair.getFirst();
        final Map<Long, Integer> dataSet = blockDataPair.getSecond();

        // remove a block from the MemoryStore
        removeBlockFromMemoryStore(blockId);
        removedBlockCount++;

        // remove the data set of the removed block from the training data currently stored in the MemoryStore.
        for (final Long key : dataSet.keySet()) {
          final Integer expectedValue = dataSet.get(key);
          final Integer actualValue = unusedDataInStore.remove(key);
          if (actualValue != null) {
            assertEquals(expectedValue, actualValue);
            numRemovedInstances++;
          }
        }
      }

      // load the next training data from the TrainingDataProvider
      trainingData = trainingDataProvider.getNextTrainingData();
      miniBatchIdx++;
    }

    // all training data initially stored in the MemoryStore should be either used or removed.
    assertEquals(numTotalInstances, numUsedInstances + numRemovedInstances);
    assertTrue(unusedDataInStore.isEmpty());
  }

  /**
   * Test whether {@link TrainingDataProvider} provides the training data properly
   * when block addition events occur at a fixed interval during an epoch.
   */
  private List<Pair<Integer, Map<Long, Integer>>> testGetNextTrainingDataWithBlockAdditions(
      final int initialNumTotalInstances) {
    final int numBlocksToAdd = 7;
    final List <Integer> blockIdList = generateBlockIdNotInLocalStore();
    final List<Pair<Integer, Map<Long, Integer>>> blocksToAdd = new ArrayList<>();

    // generate data sets for blocks to be added
    for (int i = 0; i < numBlocksToAdd; i++) {
      final int blockId = blockIdList.get(i);
      final Map<Long, Integer> dataSet = generateDataSetForBlock(blockId, BLOCK_SIZE);
      blocksToAdd.add(new Pair<>(blockId, dataSet));
    }

    final int numTotalInstances = initialNumTotalInstances + numBlocksToAdd * BLOCK_SIZE;
    final int remainderForLastMiniBatch = numTotalInstances % MINI_BATCH_SIZE;
    final int numMiniBatches = numTotalInstances / MINI_BATCH_SIZE + (remainderForLastMiniBatch == 0 ? 0 : 1);
    final int sizeOfLastMiniBatch = remainderForLastMiniBatch == 0 ? MINI_BATCH_SIZE : remainderForLastMiniBatch;

    int miniBatchIdx = 0;
    int sizeOfCurrentMiniBatch = 0;
    int numUsedInstances = 0;
    int addedBlockCount = 0;

    Map<Long, Integer> trainingData = trainingDataProvider.getNextTrainingData();
    while (!trainingData.isEmpty()) {
      // add a new block to the MemoryStore every third mini-batch including 0th
      if ((miniBatchIdx % 3 == 0) && (addedBlockCount < numBlocksToAdd)) {
        final Pair<Integer, Map<Long, Integer>> blockDataPair = blocksToAdd.get(addedBlockCount++);
        final int blockId = blockDataPair.getFirst();
        final Map<Long, Integer> dataSet = blockDataPair.getSecond();

        // add a new block to the MemoryStore
        putBlockToMemoryStore(blockId, dataSet);
      }

      // compare the training data with the expected data set
      final Set<Long> keySet = trainingData.keySet();
      for (final Long key : keySet) {
        assertEquals(memoryStore.get(key).getSecond(), trainingData.get(key));
      }

      // update the number of used instances by the size of training data
      sizeOfCurrentMiniBatch = trainingData.size();
      numUsedInstances += sizeOfCurrentMiniBatch;
      trainingData = trainingDataProvider.getNextTrainingData();
      miniBatchIdx++;
    }

    assertEquals(numTotalInstances, numUsedInstances);
    assertEquals(numMiniBatches, miniBatchIdx);
    assertEquals(sizeOfLastMiniBatch, sizeOfCurrentMiniBatch);
    return blocksToAdd;
  }

  /**
   * Generates training data to be stored in a specified block.
   * @param blockId id of the block where generated data will be stored
   * @param dataSetSize the size of generated data
   * @return a generated data set in form of key-value map
   */
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

  /**
   * Generates a list of block ids which don't belong to the local store.
   * @return a list of block ids not belonging in the local store
   */
  private List<Integer> generateBlockIdNotInLocalStore() {
    final List<Integer> blockIdsInLocalStore = ownershipCache.getCurrentLocalBlockIds();
    final List<Integer> blockIdList = new ArrayList<>();
    final int minBlockId = MIN_BLOCK_ID;
    final int maxBlockId = MIN_BLOCK_ID + NUM_TOTAL_BLOCKS - 1;
    for (int blockId = minBlockId; blockId <= maxBlockId; blockId++) {
      if (blockIdsInLocalStore.contains(blockId)) {
        continue;
      }
      blockIdList.add(blockId);
    }
    return blockIdList;
  }
}
