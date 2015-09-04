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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.TestUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Thread safeness checks for PartitionManager.
 */
public final class PartitionManagerTest {

  // Before use, put index at the end of these prefixes.
  // For single evaluator or single data type case, just use these prefixes without change for convenience.
  private static final String EVAL_ID_PREFIX = "Evaluator-";
  private static final String DATA_TYPE_PREFIX = "DATA_TYPE_";
  private static final String MSG_SIZE_ASSERTION = "size of final partition manager";
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";

  private PartitionManager partitionManager;

  @Before
  public void setUp() {
    try {
      partitionManager = Tang.Factory.getTang().newInjector().getInstance(PartitionManager.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting PartitionManager", e);
    }
  }

  /**
   * Testing multi-thread addition on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddDisjointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread removal on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadRemoveDisjointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 100000;
    final int totalNumberOfRemoves = numThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreads, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddGetDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfAdds = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[2 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread removal and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int removesPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfRemoves = numThreadsPerOperation * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[2 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition and removal on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add and remove disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[index + numThreadsPerOperation] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add, remove, and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Use more than one evaluators to test this.
   * Check that the consistency of a MemoryStore is preserved when multiple evaluators
   * on multiple threads try to add, remove, and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadMultiEvaluatorAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use evaluator-0 to evaluator-7, total 8 evaluator ids. (same as numThreadsPerOperation)
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        partitionManager.registerPartition(EVAL_ID_PREFIX + j, DATA_TYPE_PREFIX, 2 * itemIndex, 2 * itemIndex + 1);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    // Be careful with handling indexes to completely remove exact objects in exact evaluator ids.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    int realNumberOfObjects = 0;
    for (int index = 0; index < numThreadsPerOperation; index++) {
      realNumberOfObjects += partitionManager.getRangeSet(EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX).size();
    }
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2, realNumberOfObjects);
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Use more than one data types to test this.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add, remove, and retrieve disjoint ranges of data types concurrently.
   */
  @Test
  public void testMultithreadMultiDataTypeAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use DATA_TYPE_0 to DATA_TYPE_7, total 8 data types. (same as numThreadsPerOperation)
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX + j, 2 * itemIndex, 2 * itemIndex + 1);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    // Be careful with handling indexes to completely remove exact objects in exact data types.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    int realNumberOfObjects = 0;
    for (int index = 0; index < numThreadsPerOperation; index++) {
      realNumberOfObjects += partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index).size();
    }
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2, realNumberOfObjects);
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Use more than one evaluators and data types to test this.
   * Check that the consistency of a MemoryStore is preserved when multiple evaluators
   * on multiple threads try to add, remove, and retrieve disjoint ranges of data types concurrently.
   */
  @Test
  public void testMultithreadMultiEvaluatorMultiDataTypeAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use 4 evaluators and 2 data types, total 8 cases. (same as numThreadsPerOperation)
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        final int evalIndex = j / 2 % 4;
        final int dataTypeIndex = j % 2;
        partitionManager.registerPartition(EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex,
            2 * itemIndex, 2 * itemIndex + 1);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    // Be careful with handling indexes to completely remove exact objects in exact evaluators and data types.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      final int evalIndex = index / 2 % 4;
      final int dataTypeIndex = index % 2;
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex);
      threads[3 * index + 1] = new GetThread(countDownLatch, partitionManager, getsPerThread,
          EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    int realNumberOfObjects = 0;
    for (int i = 0; i < numThreadsPerOperation; i++) {
      final int evalIndex = i / 2 % 4;
      final int dataTypeIndex = i % 2;
      realNumberOfObjects
          += partitionManager.getRangeSet(EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex).size();
    }
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2, realNumberOfObjects);
  }

  private enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  final class RegisterThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int myIndex;
    private final int numThreads;
    private final int addsPerThread;
    private final IndexParity indexParity;
    private final String evalId;
    private final String dataType;

    RegisterThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
                   final int myIndex, final int numThreads, final int addsPerThread, final IndexParity indexParity,
                   final String evalId, final String dataType) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.addsPerThread = addsPerThread;
      this.indexParity = indexParity;
      this.evalId = evalId;
      this.dataType = dataType;
    }

    @Override
    public void run() {
      for (int i = 0; i < addsPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1) {
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        final int itemIndex = numThreads * i + myIndex;
        partitionManager.registerPartition(evalId, dataType,
            new LongRange(2 * itemIndex, 2 * itemIndex + 1));
      }
      countDownLatch.countDown();
    }
  }

  final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int myIndex;
    private final int numThreads;
    private final int removesPerThread;
    private final IndexParity indexParity;
    private final String evalId;
    private final String dataType;

    RemoveThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
                 final int myIndex, final int numThreads, final int removesPerThread,
                 final IndexParity indexParity, final String evalId, final String dataType) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.removesPerThread = removesPerThread;
      this.indexParity = indexParity;
      this.evalId = evalId;
      this.dataType = dataType;
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

        final int itemIndex = numThreads * i + myIndex;
        partitionManager.remove(evalId, dataType,
            new LongRange(2 * itemIndex, 2 * itemIndex + 1));
      }
      countDownLatch.countDown();
    }
  }

  class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int getsPerThread;
    private final String evalId;
    private final String dataType;

    GetThread(final CountDownLatch countDownLatch,
              final PartitionManager partitionManager,
              final int getsPerThread, final String evalId, final String dataType) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.getsPerThread = getsPerThread;
      this.evalId = evalId;
      this.dataType = dataType;
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final Set<LongRange> rangeSet =  partitionManager.getRangeSet(evalId, dataType);
        if (rangeSet == null) {
          continue;
        }

        // We make sure this thread actually iterates over the returned list, so that
        // we can check if other threads writing on the backing list affect this thread.
        for (final LongRange longRange : rangeSet) {
          longRange.getMinimumLong();
        }
      }
      countDownLatch.countDown();
    }
  }
}
