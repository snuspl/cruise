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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Thread safeness checks for PartitionManager.
 */
public final class PartitionManagerTest {


  // Put evaluator id or data type index at the end of these prefixes before use.
  // For single evaluator or single data type case, just use these prefixes without changing them for convenience.
  private static final String EVAL_ID_PREFIX = "Evaluator-";
  private static final String DATA_TYPE_PREFIX = "DATA_TYPE_";
  private static final String MSG_REGISTER_UNEXPECTED_RESULT = "unexpected result when registering a partition";
  private static final String MSG_SIZE_ASSERTION = "size of final partition manager";
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_RANGE_INCORRECT = "partition manager returns incorrect range";

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
   * Testing multi-thread addition on contiguous id ranges.
   * Check that the partitions are properly merged
   * when multiple threads try to add contiguous ranges concurrently.
   */
  @Test
  public void testMultiThreadAddContiguousRanges() throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final long rangeTerm = 2;
    final long rangeLength = 2;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads * 2);

    final Runnable[] threads0 = new Runnable[numThreads];
    final Runnable[] threads1 = new Runnable[numThreads];
    final int gap = 100;

    // register contiguous partitions, which will be merged into single partition
    for (int index = 0; index < numThreads; index++) {
      threads0[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }

    // register contiguous partitions but has certain distance from the partitions of threads0
    final long secondStartIdx = numThreads * addsPerThread + gap;
    for (int index = 0; index < numThreads; index++) {
      threads1[index] = new RegisterThread(countDownLatch, partitionManager,
          (int) secondStartIdx + index, numThreads, addsPerThread, IndexParity.ALL_INDEX,
          EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0
      );
    }

    TestUtils.runConcurrently(threads0);
    TestUtils.runConcurrently(threads1);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);

    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 2, partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());

    // check that the merged partitions has correct ranges
    final Iterator<LongRange> iterator = partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).iterator();
    final LongRange range0 = iterator.next();
    final LongRange range1 = iterator.next();
    assertEquals(MSG_RANGE_INCORRECT, new LongRange(0, rangeLength * totalNumberOfAdds - 1), range0);
    assertEquals(MSG_RANGE_INCORRECT, new LongRange(rangeLength * secondStartIdx,
        rangeLength * secondStartIdx + rangeLength * totalNumberOfAdds - 1), range1);
  }

  /**
   * Testing addition on joint id ranges.
   * Check that the partition can filter the registering ranges
   * when they are joint with other existing ranges.
   */
  @Test
  public void testMultiThreadAddJointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final long rangeTerm = 4;
    final long rangeLength = 2;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads * 2);

    final Runnable[] threads0 = new Runnable[numThreads];
    final Runnable[] threads1 = new Runnable[numThreads];

    // partitions of threads0 and threads1 are disjoint by its own,
    // but they are 1 to 1 joint for each other.
    // Therefore, only one-side of partitions can be registered.
    for (int index = 0; index < numThreads; index++) {
      threads0[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }

    for (int index = 0; index < numThreads; index++) {
      threads1[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 1);
    }

    TestUtils.runConcurrently(threads0);
    TestUtils.runConcurrently(threads1);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);

    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing addition on joint id ranges.
   * Check that the partition can filter the registering ranges
   * when they are joint with other existing ranges.
   */
  @Test
  public void testAddJointRanges() throws InterruptedException {
    final int totalNumberOfAdds = 100000;

    for (int i = 0; i < totalNumberOfAdds; i++) {
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    for (int i = 0; i < totalNumberOfAdds; i++) {
      assertFalse(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i + 1, 3 * i + 2));
    }
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition on duplicate id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add duplicate ranges concurrently.
   */
  @Test
  public void testMultiThreadAddDuplicateRanges() throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final int dupFactor = 2; // adjust it
    final int effectiveThreads = numThreads / dupFactor + ((numThreads % dupFactor == 0) ? 0 : 1);
    final int totalNumberOfAdds = effectiveThreads * addsPerThread;

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index / dupFactor, effectiveThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add disjoint ranges concurrently.
   */
  @Test
  public void testMultiThreadAddDisjointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread removal on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove disjoint ranges concurrently.
   */
  @Test
  public void testMultiThreadRemoveDisjointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 100000;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfRemoves = numThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreads, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
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
  public void testMultiThreadAddGetDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfAdds = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
      threads[2 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfAdds,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread removal and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultiThreadGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int removesPerThread = 100000;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfRemoves = numThreadsPerOperation * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
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
  public void testMultiThreadAddRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
      threads[index + numThreadsPerOperation] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfObjects / 2,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add, remove, and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultiThreadAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.registerPartition(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, 0);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION,
        totalNumberOfObjects / 2,
        partitionManager.getRangeSet(EVAL_ID_PREFIX, DATA_TYPE_PREFIX).size());
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
    private final long rangeTerm;
    private final long rangeLength;
    private int offset;

    RegisterThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
                   final int myIndex, final int numThreads, final int addsPerThread, final IndexParity indexParity,
                   final String evalId, final String dataType,
                   final long rangeTerm, final long rangeLength, final int offset) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.addsPerThread = addsPerThread;
      this.indexParity = indexParity;
      this.evalId = evalId;
      this.dataType = dataType;
      this.rangeTerm = rangeTerm;
      this.rangeLength = rangeLength;
      this.offset = offset;
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
            new LongRange(rangeTerm * itemIndex + offset, rangeTerm * itemIndex + offset + (rangeLength - 1)));
      }
      countDownLatch.countDown();
    }
  }

  final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int myIndex;
    private final long rangeTerm;
    private final long rangeLength;
    private final int offset;
    private final int numThreads;
    private final int removesPerThread;
    private final IndexParity indexParity;
    private final String evalId;
    private final String dataType;

    RemoveThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
                 final int myIndex, final int numThreads, final int removesPerThread,
                 final IndexParity indexParity, final String evalId, final String dataType,
                 final long rangeTerm, final long rangeLength, final int offset) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.removesPerThread = removesPerThread;
      this.indexParity = indexParity;
      this.evalId = evalId;
      this.dataType = dataType;
      this.rangeTerm = rangeTerm;
      this.rangeLength = rangeLength;
      this.offset = offset;
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
            new LongRange(rangeTerm * itemIndex + offset, rangeTerm * itemIndex + offset + (rangeLength - 1)));
      }
      countDownLatch.countDown();
    }
  }

  final class GetThread implements Runnable {
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
