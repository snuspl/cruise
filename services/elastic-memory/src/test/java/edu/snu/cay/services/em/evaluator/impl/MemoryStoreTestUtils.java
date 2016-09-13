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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.BlockUpdateListener;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Utilities for testing MemoryStore.
 */
public final class MemoryStoreTestUtils {

  public enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  public static final class PutThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int putsPerThread;
    private final int itemsPerPut;
    private final IndexParity indexParity;

    public PutThread(final CountDownLatch countDownLatch, final MemoryStore memoryStore,
                     final int myIndex, final int numThreads, final int putsPerThread, final int itemsPerPut,
                     final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.putsPerThread = putsPerThread;
      this.itemsPerPut = itemsPerPut;
      this.indexParity = indexParity;
    }

    @Override
    public void run() {
      for (int i = 0; i < putsPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1) {
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        if (itemsPerPut == 1) {
          final long itemIndex = numThreads * i + myIndex;
          memoryStore.put(itemIndex, i);
        } else {
          final long itemStartIndex = (numThreads * i + myIndex) * itemsPerPut;
          final List<Long> ids = new ArrayList<>(itemsPerPut);
          final List<Long> values = new ArrayList<>(itemsPerPut);
          for (long itemIndex = itemStartIndex; itemIndex < itemStartIndex + itemsPerPut; itemIndex++) {
            ids.add(itemIndex);
            values.add(itemIndex);
          }
          memoryStore.putList(ids, values);
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int removesPerThread;
    private final int itemsPerRemove;
    private final IndexParity indexParity;

    public RemoveThread(final CountDownLatch countDownLatch, final MemoryStore memoryStore,
                        final int myIndex, final int numThreads, final int removesPerThread, final int itemsPerRemove,
                        final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.removesPerThread = removesPerThread;
      this.itemsPerRemove = itemsPerRemove;
      this.indexParity = indexParity;
    }

    @Override
    public void run() {
      for (int i = 0; i < removesPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1) {
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        if (itemsPerRemove == 1) {
          final long itemIndex = numThreads * i + myIndex;
          memoryStore.remove(itemIndex);
        } else {
          final long itemStartIndex = (numThreads * i + myIndex) * itemsPerRemove;
          final long itemEndIndex = itemStartIndex + itemsPerRemove - 1;
          memoryStore.removeRange(itemStartIndex, itemEndIndex);
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int getsPerThread;
    private final int totalNumberOfObjects;
    private final boolean testRange;
    private final Random random;

    public GetThread(final CountDownLatch countDownLatch, final MemoryStore memoryStore,
                     final int getsPerThread, final boolean testRange,
                     final int totalNumberOfObjects) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.getsPerThread = getsPerThread;
      this.testRange = testRange;
      this.totalNumberOfObjects = totalNumberOfObjects;
      this.random = new Random();
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final int getMethod = testRange ? random.nextInt(3) : 0;
        if (getMethod == 0) {
          memoryStore.get((long) random.nextInt(totalNumberOfObjects));

        } else if (getMethod == 1) {
          final int startId = random.nextInt(totalNumberOfObjects);
          final int endId = random.nextInt(totalNumberOfObjects - startId) + startId;

          final Map<Long, Object> subMap = memoryStore.getRange((long) startId, (long) endId);
          if (subMap == null) {
            continue;
          }

          // We make sure this thread actually iterates over the returned map, so that
          // we can check if other threads writing on the backing map affect this thread.
          for (final Map.Entry entry : subMap.entrySet()) {
            entry.getKey();
          }

        } else {
          final Map<Long, Object> allMap = memoryStore.getAll();
          if (allMap == null) {
            continue;
          }

          // We make sure this thread actually iterates over the returned map, so that
          // we can check if other threads writing on the backing map affect this thread.
          for (final Map.Entry entry : allMap.entrySet()) {
            entry.getKey();
          }
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class UpdateThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final long startKey;
    private final int numKeys;
    private final int deltaValue;
    private final int updatesPerKeyPerThread;

    public UpdateThread(final CountDownLatch countDownLatch, final MemoryStore memoryStore,
                        final long startKey, final int numKeys, final int deltaValue,
                        final int updatesPerKeyPerThread) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.startKey = startKey;
      this.numKeys = numKeys;
      this.deltaValue = deltaValue;
      this.updatesPerKeyPerThread = updatesPerKeyPerThread;
    }

    @Override
    public void run() {
      for (int i = 0; i < updatesPerKeyPerThread; i++) {
        for (long key = startKey; key < startKey + numKeys; key++) {
          memoryStore.update(key, deltaValue);
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class BlockAddListenerImpl implements BlockUpdateListener<Long> {
    private final CountDownLatch countDownLatch;
    private final int numOfKeysPerBlock;

    /**
     * a constructor of {@link BlockAddListenerImpl}.
     * @param countDownLatch a count down latch which will count down by 1 at each block put event
     * @param numOfKeysPerBlock the number of keys in a block
     *                          used to check the updated block's key set is same with an expected key set.
     */
    public BlockAddListenerImpl(final CountDownLatch countDownLatch, final int numOfKeysPerBlock) {
      this.countDownLatch = countDownLatch;
      this.numOfKeysPerBlock = numOfKeysPerBlock;
    }

    @Override
    public void onAddedBlock(final int blockId, final Set<Long> addedKeys) {

      // check the update block has the same key set with an expected key set.
      assertEquals(numOfKeysPerBlock, addedKeys.size());

      final int keyIdBase = blockId * numOfKeysPerBlock;
      for (int j = 0; j < numOfKeysPerBlock; j++) {
        final int keyId = keyIdBase + j;
        assertTrue(addedKeys.contains(new Long((long)keyId)));
      }

      countDownLatch.countDown();
    }

    @Override
    public void onRemovedBlock(final int blockId, final Set<Long> removedKeys) {
      // do nothing
    }
  }

  public static final class BlockRemoveListenerImpl implements BlockUpdateListener<Long> {
    private final CountDownLatch countDownLatch;
    private final int numOfKeysPerBlock;

    /**
     * a constructor of {@link BlockRemoveListenerImpl}.
     * @param countDownLatch a count down latch which will count down by 1 at each block remove event
     * @param numOfKeysPerBlock the number of keys in a block
     *                          used to check the updated block's key set is same with an expected key set.
     */
    public BlockRemoveListenerImpl(final CountDownLatch countDownLatch, final int numOfKeysPerBlock) {
      this.countDownLatch = countDownLatch;
      this.numOfKeysPerBlock = numOfKeysPerBlock;
    }

    @Override
    public void onAddedBlock(final int blockId, final Set<Long> addedKeys) {
      // do nothing
    }

    @Override
    public void onRemovedBlock(final int blockId, final Set<Long> removedKeys) {
      // check the update block has the same key set with an expected key set.
      assertEquals(numOfKeysPerBlock, removedKeys.size());

      final int keyIdBase = blockId * numOfKeysPerBlock;
      for (int j = 0; j < numOfKeysPerBlock; j++) {
        final int keyId = keyIdBase + j;
        assertTrue(removedKeys.contains(new Long((long)keyId)));
      }

      countDownLatch.countDown();
    }
  }
}
