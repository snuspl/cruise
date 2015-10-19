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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.TestUtils;
import edu.snu.cay.services.em.evaluator.api.SubMemoryStore;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test class for checking the thread safeness of SubMemoryStore.
 */
public final class SubMemoryStoreTest {

  private static final String DATA_TYPE = "DATA_TYPE";
  private static final String MSG_SIZE_ASSERTION = "size of final memory store";
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_REMOVE_ALL_ASSERTION = "getAll() after removeAll()";

  private SubMemoryStore subMemoryStore;

  @Before
  public void setUp() {
    subMemoryStore = new SubMemoryStoreImpl();
  }

  /**
   * Multithreading test for {@code put}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to put single objects concurrently.
   */
  @Test
  public void testMultithreadPutSingle() throws InterruptedException {
    final int numThreads = 8;
    final int putsPerThread = 100000;
    final int totalNumberOfObjects = numThreads * putsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new PutThread(
          countDownLatch, subMemoryStore, index, numThreads, putsPerThread, 1, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        subMemoryStore.getAll(DATA_TYPE).size());
  }

  /**
   * Multithreading test for {@code remove}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to remove single objects concurrently.
   */
  @Test
  public void testMultithreadRemoveSingle() throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 100000;
    final int totalNumberOfObjects = numThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    for (int i = 0; i < totalNumberOfObjects; i++) {
      subMemoryStore.put(DATA_TYPE, i, i);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RemoveThread(
          countDownLatch, subMemoryStore, index, numThreads, removesPerThread, 1, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, subMemoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutRemoveSingle() throws InterruptedException {
    mulithreadPutRemove(1);
  }

  @Test
  public void testMultithreadPutRemoveRanges() throws InterruptedException {
    mulithreadPutRemove(10);
  }

  /**
   * Multithreading test for {@code put} and {@code remove}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to put and remove objects concurrently.
   */
  private void mulithreadPutRemove(final int itemsPerPutOrRemove) throws InterruptedException {
    final int numThreadPerOperation = 8;
    final int itemsPerThread = 100000;
    final int totalNumberOfObjects = numThreadPerOperation * itemsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadPerOperation);

    for (int i = 0; i < totalNumberOfObjects; i++) {
      if (i / numThreadPerOperation / itemsPerPutOrRemove % 2 == 0) {
        continue;
      }
      subMemoryStore.put(DATA_TYPE, i, i);
    }

    final Runnable[] threads = new Runnable[2 * numThreadPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadPerOperation; index++) {
      threads[2 * index] = new PutThread(countDownLatch, subMemoryStore, index,
          numThreadPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove, IndexParity.EVEN_INDEX);
      threads[2 * index + 1] = new RemoveThread(countDownLatch, subMemoryStore, index,
          numThreadPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove, IndexParity.ODD_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        subMemoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        subMemoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, subMemoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutGetSingle() throws InterruptedException {
    multithreadPutGet(1);
  }

  @Test
  public void testMultithreadPutGetRanges() throws InterruptedException {
    multithreadPutGet(10);
  }

  /**
   * Multithreading test for {@code put} and {@code get}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to put and get objects concurrently.
   */
  private void multithreadPutGet(final int itemsPerPut) throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int itemsPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * itemsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new PutThread(countDownLatch, subMemoryStore,
          index, numThreadsPerOperation, itemsPerThread / itemsPerPut, itemsPerPut, IndexParity.ALL_INDEX);
      threads[2 * index + 1] = new GetThread(countDownLatch, subMemoryStore, getsPerThread, totalNumberOfObjects);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        subMemoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        subMemoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, subMemoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadGetRemoveSingle() throws InterruptedException {
    multithreadGetRemove(1);
  }

  @Test
  public void testMultithreadGetRemoveRanges() throws InterruptedException {
    multithreadGetRemove(10);
  }

  /**
   * Multithreading test for {@code get} and {@code remove}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to get and remove objects concurrently.
   */
  private void multithreadGetRemove(final int itemsPerRemove) throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int itemsPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * itemsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    for (int i = 0; i < totalNumberOfObjects; i++) {
      subMemoryStore.put(DATA_TYPE, i, i);
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RemoveThread(countDownLatch, subMemoryStore,
          index, numThreadsPerOperation, itemsPerThread / itemsPerRemove, itemsPerRemove, IndexParity.ALL_INDEX);
      threads[2 * index + 1] = new GetThread(countDownLatch, subMemoryStore, getsPerThread, totalNumberOfObjects);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, subMemoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutGetRemoveSingle() throws InterruptedException {
    multithreadPutGetRemove(1);
  }

  @Test
  public void testMultithreadPutGetRemoveRanges() throws InterruptedException {
    multithreadPutGetRemove(10);
  }

  /**
   * Multithreading test for {@code put}, {@code get}, and {@code remove}.
   * Check that the consistency of a {@code SubMemoryStore} is preserved
   * when multiple threads try to put, get, and remove objects concurrently.
   */
  private void multithreadPutGetRemove(final int itemsPerPutOrRemove) throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int itemsPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * itemsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    for (int i = 0; i < totalNumberOfObjects; i++) {
      if (i / numThreadsPerOperation / itemsPerPutOrRemove % 2 == 0) {
        continue;
      }
      subMemoryStore.put(DATA_TYPE, i, i);
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new PutThread(countDownLatch, subMemoryStore, index,
          numThreadsPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove, IndexParity.EVEN_INDEX);
      threads[3 * index + 1] = new RemoveThread(countDownLatch, subMemoryStore, index,
          numThreadsPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove, IndexParity.ODD_INDEX);
      threads[3 * index + 2] = new GetThread(countDownLatch, subMemoryStore, getsPerThread, totalNumberOfObjects);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        subMemoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        subMemoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, subMemoryStore.getAll(DATA_TYPE).size());
  }

  private enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  final class PutThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final SubMemoryStore subMemoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int putsPerThread;
    private final int itemsPerPut;
    private final IndexParity indexParity;

    PutThread(final CountDownLatch countDownLatch,
              final SubMemoryStore subMemoryStore,
              final int myIndex, final int numThreads, final int putsPerThread, final int itemsPerPut,
              final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.subMemoryStore = subMemoryStore;
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
          final int itemIndex = numThreads * i + myIndex;
          subMemoryStore.put(DATA_TYPE, itemIndex, i);
        } else {
          final int itemStartIndex = (numThreads * i + myIndex) * itemsPerPut;
          final List<Long> ids = new ArrayList<>(itemsPerPut);
          final List<Integer> values = new ArrayList<>(itemsPerPut);
          for (int itemIndex = itemStartIndex; itemIndex < itemStartIndex + itemsPerPut; itemIndex++) {
            ids.add((long)itemIndex);
            values.add(itemIndex);
          }
          subMemoryStore.putList(DATA_TYPE, ids, values);
        }
      }

      countDownLatch.countDown();
    }
  }

  final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final SubMemoryStore subMemoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int removesPerThread;
    private final int itemsPerRemove;
    private final IndexParity indexParity;

    RemoveThread(final CountDownLatch countDownLatch,
                 final SubMemoryStore subMemoryStore,
                 final int myIndex, final int numThreads, final int removesPerThread, final int itemsPerRemove,
                 final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.subMemoryStore = subMemoryStore;
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
          final int itemIndex = numThreads * i + myIndex;
          subMemoryStore.remove(DATA_TYPE, itemIndex);
        } else {
          final int itemStartIndex = (numThreads * i + myIndex) * itemsPerRemove;
          final int itemEndIndex = itemStartIndex + itemsPerRemove - 1;
          subMemoryStore.removeRange(DATA_TYPE, itemStartIndex, itemEndIndex);
        }
      }

      countDownLatch.countDown();
    }
  }

  final class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final SubMemoryStore subMemoryStore;
    private final int getsPerThread;
    private final int totalNumberOfObjects;
    private final Random random;

    GetThread(final CountDownLatch countDownLatch,
              final SubMemoryStore subMemoryStore,
              final int getsPerThread,
              final int totalNumberOfObjects) {
      this.countDownLatch = countDownLatch;
      this.subMemoryStore = subMemoryStore;
      this.getsPerThread = getsPerThread;
      this.totalNumberOfObjects = totalNumberOfObjects;
      this.random = new Random();
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final int getMethod = random.nextInt(3);
        if (getMethod == 0) {
          subMemoryStore.get(DATA_TYPE, random.nextInt(totalNumberOfObjects));

        } else if (getMethod == 1) {
          final int startId = random.nextInt(totalNumberOfObjects);
          final int endId = random.nextInt(totalNumberOfObjects - startId) + startId;

          final Map<Long, Object> subMap = subMemoryStore.getRange(DATA_TYPE, startId, endId);
          if (subMap == null) {
            continue;
          }

          // We make sure this thread actually iterates over the returned map, so that
          // we can check if other threads writing on the backing map affect this thread.
          for (final Map.Entry entry : subMap.entrySet()) {
            entry.getKey();
          }

        } else {
          final Map<Long, Object> allMap = subMemoryStore.getAll(DATA_TYPE);
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
}
