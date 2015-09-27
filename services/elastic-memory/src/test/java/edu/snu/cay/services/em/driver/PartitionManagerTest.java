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
   * Testing removal on small disjoint partitions using a large partition.
   * All the sub-partitions are removed.
   */
  @Test
  public void testLargeRangeRemove() {
    final String evalId = EVAL_ID_PREFIX + 0;
    final String dataType = DATA_TYPE_PREFIX + 0;

    // Multiple sub-partitions are removed when requested to remove a large partition.
    // Register disjoint ranges: [1, 2] [4, 5] [7, 8] ...
    final int numItems = 10;
    for (int i = 0; i < numItems; i++) {
      partitionManager.register(evalId, dataType, 3 * i + 1, 3 * i + 2);
    }

    // Removing range [0, 30] will remove all partitions.
    final Set<LongRange> removed = partitionManager.remove(evalId, dataType, new LongRange(0, 30));
    assertEquals(0, partitionManager.getRangeSet(evalId, dataType).size());
    assertEquals(numItems, removed.size());
  }

  /**
   * Testing removal on a large partition using a small partition.
   * The large partition is split into two pieces.
   */
  @Test
  public void testRepartitioningRemove() {
    final String evalId = EVAL_ID_PREFIX + 0;
    final String dataType = DATA_TYPE_PREFIX + 0;

    // A partition is split into two sub-partitions.
    partitionManager.register(evalId, dataType, 0, 10);
    final LongRange toRemove = new LongRange(3, 5);
    final Set<LongRange> removed = partitionManager.remove(evalId, dataType, toRemove);

    // Only this partition is returned
    assertTrue(removed.contains(toRemove));
    assertEquals(1, removed.size());

    // Remaining partitions: [0, 2] and [6, 10]
    assertEquals(2, partitionManager.getRangeSet(evalId, dataType).size());
    assertTrue(partitionManager.getRangeSet(evalId, dataType).contains(new LongRange(0, 2)));
    assertTrue(partitionManager.getRangeSet(evalId, dataType).contains(new LongRange(6, 10)));
  }

  /**
   * Testing removal on intersecting range.
   * Only part of the range will be removed.
   */
  @Test
  public void testIntersectingRangeRemove() {
    final String evalId = EVAL_ID_PREFIX + 0;
    final String dataType = DATA_TYPE_PREFIX + 0;

    // Register range [2, 4]
    partitionManager.register(evalId, dataType, 2, 4);

    // Removing [1, 3] will return [2, 3] leaving [4, 4]
    final LongRange toRemove = new LongRange(1, 3);
    final Set<LongRange> removed = partitionManager.remove(evalId, dataType, toRemove);

    assertEquals(1, removed.size());
    assertTrue(removed.contains(new LongRange(2, 3)));
    final Set<LongRange> left = partitionManager.getRangeSet(evalId, dataType);
    assertEquals(1, left.size());
    assertTrue(left.contains(new LongRange(4, 4)));
  }

  /**
   * Testing removal on intersecting range,
   * where the ranges have the same ends.
   */
  @Test
  public void testSameEndRangeRemove() {
    final String evalId = EVAL_ID_PREFIX + 0;
    final String dataType = DATA_TYPE_PREFIX + 0;

    // We initially have [1, 10]
    partitionManager.register(evalId, dataType, 1, 10);

    // Three ranges: LEFT | CENTER | RIGHT
    final LongRange left = new LongRange(1, 3);
    final LongRange center = new LongRange(4, 6);
    final LongRange right = new LongRange(7, 10);

    // Remove LEFT ([1, 3]) from [1, 10]
    final Set<LongRange> removedRanges1 = partitionManager.remove(evalId, dataType, left);

    // Only this partition is removed
    assertTrue(removedRanges1.contains(left));
    assertEquals(1, removedRanges1.size());

    // Remaining partitions: [4, 10]
    assertEquals(1, partitionManager.getRangeSet(evalId, dataType).size());
    assertTrue(partitionManager.getRangeSet(evalId, dataType).contains(new LongRange(4, 10)));

    // Let's remove the other side

    final Set<LongRange> rightRemoved = partitionManager.remove(evalId, dataType, right);

    // Only this partition is removed
    assertTrue(rightRemoved.contains(right));
    assertEquals(1, rightRemoved.size());

    // Remaining partitions: [4, 6]
    assertEquals(1, partitionManager.getRangeSet(evalId, dataType).size());
    assertTrue(partitionManager.getRangeSet(evalId, dataType).contains(new LongRange(4, 6)));

    // Let's remove the remaining range with the exact ends.

    final Set<LongRange> centerRemoved = partitionManager.remove(evalId, dataType, center);

    // Only this partition is removed
    assertTrue(centerRemoved.contains(center));
    assertEquals(1, centerRemoved.size());

    // Now the partition is empty.
    assertEquals(0, partitionManager.getRangeSet(evalId, dataType).size());
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
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i, 3 * i + 1));
    }

    for (int i = 0; i < totalNumberOfAdds; i++) {
      assertFalse(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, 3 * i + 1, 3 * i + 2));
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
    final int initialOffset = 0;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    // Register disjoint ranges: [0, 1] [3, 4] [6, 7] ...
    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
  public void testMultiThreadRemoveDisjointRanges() throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 100000;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfRemoves = numThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    // Register disjoint ranges: [0, 1] [3, 4] [6, 7] ...
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      final int rangeStart = (int) (initialOffset + rangeTerm * i);
      final int rangeEnd = (int) (rangeStart + rangeLength - 1);
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, rangeStart, rangeEnd));
    }

    // Remove disjoint ranges: [0, 1] [3, 4] [6, 7] ...
    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreads, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
    final int initialOffset = 0;
    final int totalNumberOfAdds = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      // Register disjoint ranges [0, 1] [3, 4] [6, 7] ...
      threads[2 * index] = new RegisterThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, addsPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
  public void testMultiThreadGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int removesPerThread = 100000;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfRemoves = numThreadsPerOperation * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    // Register disjoint ranges [0, 1] [3, 4] [6, 7] ...
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      final int rangeStart = (int) (initialOffset + rangeTerm * i);
      final int rangeEnd = (int) (rangeStart + rangeLength - 1);
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, rangeStart, rangeEnd));
    }

    // Threads with even index remove disjoint ranges [0, 1] [3, 4] [6, 7] ...
    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ALL_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
    final int initialOffset = 0;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(2 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Register disjoint ranges [3, 4] [9, 10] [15, 16]...
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      final int rangeStart = (int) (initialOffset + rangeTerm * i);
      final int rangeEnd = (int) (rangeStart + rangeLength - 1);
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, rangeStart, rangeEnd));
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
          rangeTerm, rangeLength, initialOffset);
      threads[index + numThreadsPerOperation] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
  public void testMultiThreadAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Register disjoint ranges [0, 1] [3, 4] [6, 7] ...
    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      final int rangeStart = (int) (initialOffset + rangeTerm * i);
      final int rangeEnd = (int) (rangeStart + rangeLength - 1);
      assertTrue(MSG_REGISTER_UNEXPECTED_RESULT,
          partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX, rangeStart, rangeEnd));
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
          rangeTerm, rangeLength, initialOffset);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager,
          index, numThreadsPerOperation, removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
   * This test use more than one evaluator id index.
   * Check that the consistency of a MemoryStore is preserved when multiple threads try to
   * add, remove, and retrieve disjoint ranges concurrently using multiple evaluator id indexes.
   */
  @Test
  public void testMultithreadMultiEvaluatorAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use evaluator-0 to evaluator-7, total 8 evaluator ids. (same as numThreadsPerOperation)
    // Register disjoint ranges [0, 1] [3, 4] [6, 7] [9, 10] ...
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        final int rangeStart = (int) (initialOffset + rangeTerm * itemIndex);
        final int rangeEnd = (int) (rangeStart + rangeLength - 1);
        partitionManager.register(EVAL_ID_PREFIX + j, DATA_TYPE_PREFIX, rangeStart, rangeEnd);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX + index, DATA_TYPE_PREFIX,
          rangeTerm, rangeLength, initialOffset);
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
   * This test use more than one data type index.
   * Check that the consistency of a MemoryStore is preserved when multiple threads try to
   * add, remove, and retrieve disjoint ranges of multiple data types concurrently.
   */
  @Test
  public void testMultithreadMultiDataTypeAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use DATA_TYPE_0 to DATA_TYPE_7, total 8 data types. (same as numThreadsPerOperation)
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        final int rangeStart = (int) (initialOffset + rangeTerm * itemIndex);
        final int rangeEnd = (int) (rangeStart + rangeLength - 1);
        partitionManager.register(EVAL_ID_PREFIX, DATA_TYPE_PREFIX + j, rangeStart, rangeEnd);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index,
          rangeTerm, rangeLength, initialOffset);
      threads[3 * index + 1]
          = new GetThread(countDownLatch, partitionManager, getsPerThread, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX, DATA_TYPE_PREFIX + index,
          rangeTerm, rangeLength, initialOffset);
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
   * This test use more than one evaluator id and data type index.
   * Check that the consistency of a MemoryStore is preserved when multiple threads try to
   * add, remove, and retrieve disjoint ranges of multiple data types concurrently using multiple evaluator id indexes.
   */
  @Test
  public void testMultithreadMultiEvaluatorMultiDataTypeAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numThreadsPerOperation = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final long rangeTerm = 3;
    final long rangeLength = 2;
    final int initialOffset = 0;
    final int totalNumberOfObjects = numThreadsPerOperation * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(3 * numThreadsPerOperation);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // Use 4 evaluators and 2 data types, total 8 cases. (same as numThreadsPerOperation)
    for (int i = 1; i < addsPerThread; i += 2) {
      for (int j = 0; j < numThreadsPerOperation; j++) {
        final int itemIndex = numThreadsPerOperation * i + j;
        final int evalIndex = j / 2 % 4;
        final int dataTypeIndex = j % 2;
        final int rangeStart = (int) (initialOffset + rangeTerm * itemIndex);
        final int rangeEnd = (int) (rangeStart + rangeLength - 1);
        partitionManager.register(EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex,
            rangeStart, rangeEnd);
      }
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      final int evalIndex = index / 2 % 4;
      final int dataTypeIndex = index % 2;
      threads[3 * index] = new RegisterThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          addsPerThread, IndexParity.EVEN_INDEX, EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex,
          rangeTerm, rangeLength, initialOffset);
      threads[3 * index + 1] = new GetThread(countDownLatch, partitionManager, getsPerThread,
          EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex);
      threads[3 * index + 2] = new RemoveThread(countDownLatch, partitionManager, index, numThreadsPerOperation,
          removesPerThread, IndexParity.ODD_INDEX, EVAL_ID_PREFIX + evalIndex, DATA_TYPE_PREFIX + dataTypeIndex,
          rangeTerm, rangeLength, initialOffset);
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
        partitionManager.register(evalId, dataType,
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
