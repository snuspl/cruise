package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.TestUtils;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.io.network.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Contains tests that check thread safeness for implementations of MemoryStore.
 */
public final class MemoryStoreTestSuite {

  private static final String KEY = "KEY";
  private static final String MSG_SIZE_ASSERTION_FAIL = "size of final memory store";
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";

  /**
   * Multi-thread addition check.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add single objects concurrently.
   */
  public static void multithreadAdd(final MemoryStore memoryStore) throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 100000;
    final int totalNumberOfAdds = numThreads * addsPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new AddThread(countDownLatch, memoryStore, index, numThreads, addsPerThread, 1, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfAdds,
        memoryStore.get(KEY).size());
  }

  /**
   * Multi-thread list addition check.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add lists concurrently.
   */
  public static void multithreadAddList(final MemoryStore memoryStore) throws InterruptedException {
    final int numThreads = 8;
    final int addsPerThread = 10000;
    final int itemsPerAdd = 10;
    final int totalNumberOfAdds = numThreads * addsPerThread * itemsPerAdd;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new AddThread(countDownLatch, memoryStore, index, numThreads, addsPerThread, itemsPerAdd, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfAdds,
        memoryStore.get(KEY).size());
  }

  /**
   * Multi-thread removal check.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove single objects concurrently.
   */
  public static void multithreadRemove(final MemoryStore memoryStore) throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 100000;
    final int totalNumberOfRemoves = numThreads * removesPerThread;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      memoryStore.putMovable(KEY, (long) i, i);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new RemoveThread(countDownLatch, memoryStore,
          index, numThreads, removesPerThread, 1, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, 0, memoryStore.get(KEY).size());
  }

  /**
   * Multi-thread list removal check.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to remove lists concurrently.
   */
  public static void multithreadRemoveList(final MemoryStore memoryStore) throws InterruptedException {
    final int numThreads = 8;
    final int removesPerThread = 10000;
    final int itemsPerRemove = 10;
    final int totalNumberOfRemoves = numThreads * removesPerThread * itemsPerRemove;
    final CountDownLatch countDownLatch = new CountDownLatch(numThreads);
    for (int i = 0; i < totalNumberOfRemoves; i++) {
      memoryStore.putMovable(KEY, (long) i, i);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] =
          new RemoveThread(countDownLatch, memoryStore,
              index, numThreads, removesPerThread, itemsPerRemove, IndexParity.ALL_INDEX);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, 0, memoryStore.get(KEY).size());
  }

  /**
   * Multi-thread addition, removal, and retrieval check.
   * Check that the consistency of a MemoryStore is preserved
   * when multiple threads try to add, remove, and retrieve lists concurrently.
   */
  public static void multithreadAddRemoveGetIterateList(final MemoryStore memoryStore) throws InterruptedException {
    final int numAddThreads = 8;
    final int numGetThreads = 8;
    final int numRemoveThreads = numAddThreads;
    final int addsPerThread = 10000;
    final int removesPerThread = addsPerThread;
    final int getsPerThread = 100;
    final int itemsPerAddOrRemove = 10;
    final int totalNumberOfObjects = numAddThreads * addsPerThread * itemsPerAddOrRemove;
    final CountDownLatch countDownLatch = new CountDownLatch(numAddThreads + numGetThreads + numRemoveThreads);

    // Start with IndexParity.ODD_INDEX objects only. (for removal)
    // IndexParity.ODD_INDEX doesn't necessarily mean the object ids are odd.
    for (int i = 1; i < removesPerThread; i += 2) {
      final int itemStartIndex = numRemoveThreads * itemsPerAddOrRemove * i;
      final List<Long> ids = new ArrayList<>(itemsPerAddOrRemove * numRemoveThreads);
      final List<Integer> values = new ArrayList<>(itemsPerAddOrRemove * numRemoveThreads);
      for (int itemIndex = itemStartIndex; itemIndex < itemStartIndex + itemsPerAddOrRemove * numRemoveThreads; itemIndex++) {
        ids.add((long)itemIndex);
        values.add(itemIndex);
      }
      memoryStore.putMovable(KEY, ids, values);
    }

    final Runnable[] threads = new Runnable[numAddThreads + numGetThreads + numRemoveThreads];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numAddThreads; index++) {
      threads[index] = new AddThread(countDownLatch, memoryStore,
          index, numAddThreads, addsPerThread, itemsPerAddOrRemove, IndexParity.EVEN_INDEX);
    }
    for (int index = 0; index < numRemoveThreads; index++) {
      threads[index + numAddThreads] = new RemoveThread(countDownLatch, memoryStore,
          index, numRemoveThreads, removesPerThread, itemsPerAddOrRemove, IndexParity.ODD_INDEX);
    }
    for (int index = 0; index < numGetThreads; index++) {
      threads[index + numAddThreads + numRemoveThreads] =
          new GetThread(countDownLatch, memoryStore, getsPerThread, 0, totalNumberOfObjects);
    }
    TestUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(60, TimeUnit.SECONDS);


    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION_FAIL, totalNumberOfObjects / 2, memoryStore.get(KEY).size());

  }

  private enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  static final class AddThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int addsPerThread;
    private final int itemsPerAdd;
    private final IndexParity indexParity;

    AddThread(final CountDownLatch countDownLatch, final MemoryStore memoryStore,
              final int myIndex, final int numThreads, final int addsPerThread, final int itemsPerAdd,
              final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.addsPerThread = addsPerThread;
      this.itemsPerAdd = itemsPerAdd;
      this.indexParity = indexParity;
    }

    @Override
    public void run() {
      for (int i = 0; i < addsPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1){
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        if (itemsPerAdd == 1) {
          final int itemIndex = numThreads * i + myIndex;
          memoryStore.putMovable(KEY, itemIndex, i);
        } else {
          final int itemStartIndex = numThreads * itemsPerAdd * i + itemsPerAdd * myIndex;
          final List<Long> ids = new ArrayList<>(itemsPerAdd);
          final List<Integer> values = new ArrayList<>(itemsPerAdd);
          for (int itemIndex = itemStartIndex; itemIndex < itemStartIndex + itemsPerAdd; itemIndex++) {
            ids.add((long)itemIndex);
            values.add(itemIndex);
          }
          memoryStore.putMovable(KEY, ids, values);
        }
      }
      countDownLatch.countDown();
    }
  }

  static final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int myIndex;
    private final int numThreads;
    private final int removesPerThread;
    private final int itemsPerRemove;
    private final IndexParity indexParity;

    RemoveThread(final CountDownLatch countDownLatch,
                 final MemoryStore memoryStore,
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
          final int itemIndex = numThreads * i + myIndex;
          memoryStore.remove(KEY, itemIndex);
        } else {
          final int itemStartIndex = numThreads * itemsPerRemove * i + itemsPerRemove * myIndex;
          final int itemEndIndex = itemStartIndex + itemsPerRemove - 1;
          memoryStore.remove(KEY, itemStartIndex, itemEndIndex);
        }
      }

      countDownLatch.countDown();
    }
  }

  static final class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final int getsPerThread;
    private final int minIndex;
    private final int maxIndex;
    private final Random random;

    GetThread(final CountDownLatch countDownLatch,
              final MemoryStore memoryStore,
              final int getsPerThread,
              final int minIndex,
              final int maxIndex) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.getsPerThread = getsPerThread;
      this.minIndex = minIndex;
      this.maxIndex = maxIndex;
      this.random = new Random();
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final int startIndex = random.nextInt(maxIndex - minIndex) + minIndex;
        final int endIndex = random.nextInt(maxIndex - startIndex) + startIndex;
        final List<Pair<Long, Object>> pairList = memoryStore.get(KEY, startIndex, endIndex);
        if (pairList == null) {
          continue;
        }

        // We make sure this thread actually iterates over the returned list, so that
        // we can check if other threads writing on the backing list affect this thread.
        for (final Pair<Long, Object> pair : pairList) {
        }
      }
      countDownLatch.countDown();
    }
  }
}
