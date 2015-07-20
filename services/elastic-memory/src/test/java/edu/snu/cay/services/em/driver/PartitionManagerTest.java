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

  private static final String EVAL_ID = "Evaluator-1";
  private static final String KEY = "KEY";
  private static final String MSG_SIZE_ASSERTION_FAIL = "size of final partition manager";
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
      threads[index] = new AddThread(countDownLatch, partitionManager,
          index, numThreads, addsPerThread, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfAdds, partitionManager.getRangeSet(EVAL_ID, KEY).size());
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
      partitionManager.registerPartition(EVAL_ID, KEY, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] =new RemoveThread(countDownLatch, partitionManager,
          index, numThreads, removesPerThread, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, 0, partitionManager.getRangeSet(EVAL_ID, KEY).size());
  }

  /**
   * Testing multi-thread addition and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddGetDisjointRanges() throws InterruptedException {
    final int numAddThreads = 8;
    final int numGetThreads = 8;
    final int addsPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfAdds = numAddThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numAddThreads + numGetThreads);

    final Runnable[] threads = new Runnable[numAddThreads + numGetThreads];
    for (int index = 0; index < numAddThreads; index++) {
      threads[index] =new AddThread(countDownLatch, partitionManager,
          index, numAddThreads, addsPerThread, IndexParity.ALL_INDEX);
    }
    for (int index = 0; index < numGetThreads; index++) {
      threads[index + numAddThreads] = new GetThread(countDownLatch, partitionManager, getsPerThread);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfAdds, partitionManager.getRangeSet(EVAL_ID, KEY).size());
  }

  /**
   * Testing multi-thread removal and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadGetRemoveDisjointRanges() throws InterruptedException {
    final int numRemoveThreads = 8;
    final int numGetThreads = 8;
    final int removesPerThread = 100000;
    final int getsPerThread = 100;
    final int totalNumberOfRemoves = numRemoveThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numRemoveThreads + numGetThreads);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      partitionManager.registerPartition(EVAL_ID, KEY, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[numRemoveThreads + numGetThreads];
    for (int index = 0; index < numRemoveThreads; index++) {
      threads[index] = new RemoveThread(countDownLatch, partitionManager,
          index, numRemoveThreads, removesPerThread, IndexParity.ALL_INDEX);
    }
    for (int index = 0; index < numGetThreads; index++) {
      threads[index + numRemoveThreads] = new GetThread(countDownLatch, partitionManager, getsPerThread);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, 0, partitionManager.getRangeSet(EVAL_ID, KEY).size());
  }

  /**
   * Testing multi-thread addition and removal on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add and remove disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddRemoveDisjointRanges() throws InterruptedException {
    final int numAddThreads = 8;
    final int numRemoveThreads = numAddThreads;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int totalNumberOfObjects = numAddThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numAddThreads + numRemoveThreads);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      partitionManager.registerPartition(EVAL_ID, KEY, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[numAddThreads + numRemoveThreads];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numAddThreads; index++) {
      threads[index] = new AddThread(countDownLatch, partitionManager,
          index, numAddThreads, addsPerThread, IndexParity.EVEN_INDEX);
    }
    for (int index = 0; index < numRemoveThreads; index++) {
      threads[index + numAddThreads] = new RemoveThread(countDownLatch, partitionManager,
          index, numRemoveThreads, removesPerThread, IndexParity.ODD_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfObjects / 2, partitionManager.getRangeSet(EVAL_ID, KEY).size());
  }

  /**
   * Testing multi-thread addition, removal, and retrieval on disjoint id ranges.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add, remove, and retrieve disjoint ranges concurrently.
   */
  @Test
  public void testMultithreadAddGetRemoveDisjointRanges() throws InterruptedException {
    final int numAddThreads = 8;
    final int numRemoveThreads = numAddThreads;
    final int numGetThreads = 8;
    final int addsPerThread = 100000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int totalNumberOfObjects = numAddThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numAddThreads + numGetThreads + numRemoveThreads);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    for (int i = 1; i < totalNumberOfObjects; i += 2) {
      partitionManager.registerPartition(EVAL_ID, KEY, 2 * i, 2 * i + 1);
    }

    final Runnable[] threads = new Runnable[numAddThreads + numGetThreads + numRemoveThreads];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numAddThreads; index++) {
      threads[index] = new AddThread(countDownLatch, partitionManager,
          index, numAddThreads, addsPerThread, IndexParity.EVEN_INDEX);
    }
    for (int index = 0; index < numGetThreads; index++) {
      threads[index + numAddThreads] = new GetThread(countDownLatch, partitionManager, getsPerThread);
    }
    for (int index = 0; index < numRemoveThreads; index++) {
      threads[index + numAddThreads + numGetThreads] = new RemoveThread(countDownLatch, partitionManager,
          index, numRemoveThreads, removesPerThread, IndexParity.ODD_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfObjects / 2, partitionManager.getRangeSet(EVAL_ID, KEY).size());
  }

  private enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  final class AddThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int myIndex;
    private final int numThreads;
    private final int addsPerThread;
    private final IndexParity indexParity;

    AddThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
              final int myIndex, final int numThreads, final int addsPerThread, final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.addsPerThread = addsPerThread;
      this.indexParity = indexParity;
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
        partitionManager.registerPartition(EVAL_ID, KEY,
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

    RemoveThread(final CountDownLatch countDownLatch, final PartitionManager partitionManager,
                 final int myIndex, final int numThreads, final int removesPerThread,
                 final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.removesPerThread = removesPerThread;
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

        final int itemIndex = numThreads * i + myIndex;
        partitionManager.remove(EVAL_ID, KEY,
            new LongRange(2 * itemIndex, 2 * itemIndex + 1));
      }
      countDownLatch.countDown();
    }
  }

  class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final PartitionManager partitionManager;
    private final int getsPerThread;

    GetThread(final CountDownLatch countDownLatch,
              final PartitionManager partitionManager,
              final int getsPerThread) {
      this.countDownLatch = countDownLatch;
      this.partitionManager = partitionManager;
      this.getsPerThread = getsPerThread;
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final Set<LongRange> rangeSet =  partitionManager.getRangeSet(EVAL_ID, KEY);
        if (rangeSet == null) {
          continue;
        }

        // We make sure this thread actually iterates over the returned list, so that
        // we can check if other threads writing on the backing list affect this thread.
        for (final LongRange longRange : rangeSet) {
        }
      }
      countDownLatch.countDown();
    }
  }
}