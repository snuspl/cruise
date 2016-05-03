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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.impl.MemoryStoreTestUtils;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.SpanReceiver;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for checking the thread safeness of MemoryStore.
 */
public final class MemoryStoreTest {

  private static final int NUM_TOTAL_BLOCKS = 32;
  private static final int TIMEOUT = 60;

  private static final String DATA_TYPE = "DATA_TYPE";
  private static final String MSG_SIZE_ASSERTION = "size of final memory store";
  private static final String MSG_THREADS_NOT_FINISHED = "threads not finished (possible deadlock or infinite loop)";
  private static final String MSG_REMOVE_ALL_ASSERTION = "getAll() after removeAll()";

  private MemoryStore<Long> memoryStore;

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(SpanReceiver.class, MemoryStoreTestUtils.MockedSpanReceiver.class)
        .bindImplementation(ElasticMemoryMsgSender.class, MemoryStoreTestUtils.MockedMsgSender.class)
        .bindNamedParameter(KeyCodecName.class, SerializableCodec.class)
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindNamedParameter(MemoryStoreId.class, Integer.toString(0))
        .bindNamedParameter(NumInitialEvals.class, Integer.toString(1))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    memoryStore = injector.getInstance(MemoryStore.class);

    // router should be initialized explicitly
    final OperationRouter<Long> router = injector.getInstance(OperationRouter.class);
    router.initialize("DUMMY");
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
      threads[index] = new MemoryStoreTestUtils.PutThread(
          countDownLatch, memoryStore, DATA_TYPE, index, numThreads, putsPerThread, 1,
          MemoryStoreTestUtils.IndexParity.ALL_INDEX);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        memoryStore.getAll(DATA_TYPE).size());
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
      memoryStore.put(DATA_TYPE, (long) i, i);
    }

    final Runnable[] threads = new Runnable[numThreads];
    for (int index = 0; index < numThreads; index++) {
      threads[index] = new MemoryStoreTestUtils.RemoveThread(
          countDownLatch, memoryStore, DATA_TYPE, index, numThreads, removesPerThread, 1,
          MemoryStoreTestUtils.IndexParity.ALL_INDEX);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, memoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutRemoveSingle() throws InterruptedException {
    mulithreadPutRemove(1);
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
      memoryStore.put(DATA_TYPE, (long) i, i);
    }

    final Runnable[] threads = new Runnable[2 * numThreadPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadPerOperation; index++) {
      threads[2 * index] = new MemoryStoreTestUtils.PutThread(countDownLatch, memoryStore, DATA_TYPE, index,
          numThreadPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove,
          MemoryStoreTestUtils.IndexParity.EVEN_INDEX);
      threads[2 * index + 1] = new MemoryStoreTestUtils.RemoveThread(countDownLatch, memoryStore, DATA_TYPE, index,
          numThreadPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove,
          MemoryStoreTestUtils.IndexParity.ODD_INDEX);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        memoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        memoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, memoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutGetSingle() throws InterruptedException {
    multithreadPutGet(1);
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
      threads[2 * index] = new MemoryStoreTestUtils.PutThread(countDownLatch, memoryStore, DATA_TYPE,
          index, numThreadsPerOperation, itemsPerThread / itemsPerPut, itemsPerPut,
          MemoryStoreTestUtils.IndexParity.ALL_INDEX);
      threads[2 * index + 1] = new MemoryStoreTestUtils.GetThread(countDownLatch, memoryStore, DATA_TYPE,
          getsPerThread, false, totalNumberOfObjects);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        memoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects,
        memoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, memoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadGetRemoveSingle() throws InterruptedException {
    multithreadGetRemove(1);
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
      memoryStore.put(DATA_TYPE, (long) i, i);
    }

    final Runnable[] threads = new Runnable[2 * numThreadsPerOperation];
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[2 * index] = new MemoryStoreTestUtils.RemoveThread(countDownLatch, memoryStore, DATA_TYPE,
          index, numThreadsPerOperation, itemsPerThread / itemsPerRemove, itemsPerRemove,
          MemoryStoreTestUtils.IndexParity.ALL_INDEX);
      threads[2 * index + 1] = new MemoryStoreTestUtils.GetThread(countDownLatch, memoryStore, DATA_TYPE,
          getsPerThread, false, totalNumberOfObjects);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, 0, memoryStore.getAll(DATA_TYPE).size());
  }

  @Test
  public void testMultithreadPutGetRemoveSingle() throws InterruptedException {
    multithreadPutGetRemove(1);
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
      memoryStore.put(DATA_TYPE, (long) i, i);
    }

    final Runnable[] threads = new Runnable[3 * numThreadsPerOperation];
    // If we set AddThreads and RemoveThreads to add and remove the same object,
    // the behavior is non-deterministic and impossible to check.
    // Thus, we partition the objects set so that AddThreads and RemoveThreads
    // never access the same object.
    // Hence the IndexParity.EVEN_INDEX and IndexParity.ODD_INDEX.
    for (int index = 0; index < numThreadsPerOperation; index++) {
      threads[3 * index] = new MemoryStoreTestUtils.PutThread(countDownLatch, memoryStore, DATA_TYPE, index,
          numThreadsPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove,
          MemoryStoreTestUtils.IndexParity.EVEN_INDEX);
      threads[3 * index + 1] = new MemoryStoreTestUtils.RemoveThread(countDownLatch, memoryStore, DATA_TYPE, index,
          numThreadsPerOperation, itemsPerThread / itemsPerPutOrRemove, itemsPerPutOrRemove,
          MemoryStoreTestUtils.IndexParity.ODD_INDEX);
      threads[3 * index + 2] = new MemoryStoreTestUtils.GetThread(countDownLatch, memoryStore, DATA_TYPE, getsPerThread,
          false, totalNumberOfObjects);
    }
    ThreadUtils.runConcurrently(threads);
    final boolean allThreadsFinished = countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);

    // check that all threads have finished without falling into deadlocks or infinite loops
    assertTrue(MSG_THREADS_NOT_FINISHED, allThreadsFinished);
    // check that the total number of objects equal the expected number
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        memoryStore.getAll(DATA_TYPE).size());
    assertEquals(MSG_SIZE_ASSERTION, totalNumberOfObjects / 2,
        memoryStore.removeAll(DATA_TYPE).size());
    // check that removeAll works as expected
    assertEquals(MSG_REMOVE_ALL_ASSERTION, 0, memoryStore.getAll(DATA_TYPE).size());
  }
}
