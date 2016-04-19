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
package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import edu.snu.cay.utils.ThreadUtils;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The purpose of this example app is to test remote access of EM, overcoming the limitation of unit testing.
 * Tests invoke PUT/GET/REMOVE operations of memory store with data keys
 * randomly selected or issued by DataIdFactory, or statically fixed.
 * Corresponding to the ownership of data keys, operations involve remote access.
 * The tests check that all operations are performed correctly and the final state of the store is as we expected.
 */
final class RemoteEMTask implements Task {
  private static final Logger LOG = Logger.getLogger(RemoteEMTask.class.getName());

  private static final String MSG_LOCAL_SIZE_ASSERTION = "size of final local memory store";
  private static final String MSG_GLOBAL_SIZE_ASSERTION = "size of final global memory store";
  private static final String MSG_OPERATION_FAILED = "not all operations succeeded";

  private static final String DATA_TYPE = "INTEGER";

  private final MemoryStore<Long> memoryStore;
  private final DataIdFactory<Long> localDataIdFactory;

  /**
   * A router that is an internal component of EM. Here we use it in user code for testing purpose.
   */
  private final OperationRouter<Long> router;

  private final int numInitialEvals;
  private final int localMemoryStoreId;
  private final int prevRemoteStoreId;
  private final int nextRemoteStoreId;
  private final long maxDataKey;

  private final AggregationSlave aggregationSlave;
  private final EvalSideMsgHandler msgHandler;
  private final SerializableCodec<String> codec;

  private List<Pair<String, Long>> testNameToTimeList = new LinkedList<>();

  @Inject
  private RemoteEMTask(final MemoryStore<Long> memorystore,
                       final OperationRouter router,
                       final AggregationSlave aggregationSlave,
                       final EvalSideMsgHandler msgHandler,
                       final SerializableCodec<String> codec,
                       final DataIdFactory<Long> dataIdFactory,
                       @Parameter(NumInitialEvals.class) final int numInitialEvals,
                       @Parameter(MemoryStoreId.class) final int localMemoryStoreId,
                       @Parameter(NumTotalBlocks.class) final int numTotalBlocks)
      throws InjectionException {
    this.memoryStore = memorystore;
    this.router = router;
    this.aggregationSlave = aggregationSlave;
    this.msgHandler = msgHandler;
    this.codec = codec;
    this.localDataIdFactory = dataIdFactory;

    this.numInitialEvals = numInitialEvals;
    this.localMemoryStoreId = localMemoryStoreId;
    this.prevRemoteStoreId = (localMemoryStoreId - 1 + numInitialEvals) % numInitialEvals;
    this.nextRemoteStoreId = (localMemoryStoreId + 1) % numInitialEvals;
    this.maxDataKey = Long.MAX_VALUE - Long.MAX_VALUE % (Long.MAX_VALUE / numTotalBlocks) - 1;
  }

  private DataIdFactory<Long> initDataIdFactory(final int memoryStoreId) {

    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class)
        .bindNamedParameter(NumInitialEvals.class, String.valueOf(numInitialEvals))
        .bindNamedParameter(MemoryStoreId.class, String.valueOf(memoryStoreId))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    try {
      return injector.getInstance(DataIdFactory.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Synchronizes all tasks with a barrier in driver. Using this method, workers can share same view on stores for each
   * step.
   */
  private void synchronize() {
    aggregationSlave.send(RemoteEMDriver.AGGREGATION_CLIENT_ID, codec.encode(DriverSideMsgHandler.SYNC_WORKERS));
    LOG.info("SYNC START");
    msgHandler.waitForMessage();
    LOG.info("SYNC END");
  }

  /**
   * Synchronizes all tasks with a barrier in driver. Also it sends {@code count} for driver to aggregate in the
   * synchronization. As a response, driver sends a data, the sum of all counts from tasks.
   * Using this method, workers can share global state of memory stores.
   */
  private long syncGlobalCount(final long count) {
    aggregationSlave.send(RemoteEMDriver.AGGREGATION_CLIENT_ID, codec.encode(Long.toString(count)));
    return msgHandler.waitForMessage();
  }

  /**
   * Cleans up memory store after each test.
   */
  private void cleanUp() {
    // wait until previous test are finished completely
    synchronize();
    memoryStore.removeAll(DATA_TYPE);

    // wait until all stores are cleaned up
    synchronize();
  }

  public byte[] call(final byte[] memento) throws InterruptedException, IdGenerationException {

    LOG.info("RemoteEMTask commencing...");

    runTest(new TestRandomPutGetRemove());
    runTest(new TestMultiThreadRemotePutSingle());
    runTest(new TestMultiThreadRemotePutRange());
    runTest(new TestMultiThreadRemotePutGetSingle());
    runTest(new TestMultiThreadRemotePutGetRange());
    runTest(new TestMultiThreadRemotePutGetRemoveSingle());
    runTest(new TestMultiThreadRemotePutGetRemoveRange());
    runTest(new TestMultiThreadLocalPut());
    runTest(new TestMultiThreadRelayedPutSingle());
    runTest(new TestSimpleScenario());

    printResult();

    return null;
  }

  private void runTest(final Test test) {
    final long startTime = System.currentTimeMillis();
    LOG.log(Level.INFO, "Test start: {0}", test.toString());

    try {
      test.run();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Test is interrupted", e);
    } catch (final IdGenerationException e) {
      LOG.log(Level.SEVERE, "Id generation exception happens while test", e);
    }

    final long endTime = System.currentTimeMillis();

    testNameToTimeList.add(new Pair<>(test.toString(), endTime - startTime));

    LOG.log(Level.INFO, "Test end: {0}", test.toString());
    cleanUp();
  }

  private void printResult() {
    for (final Pair<String, Long> testNameToTime : testNameToTimeList) {
      LOG.log(Level.INFO, "Time elapsed: {0} ms - in {1}",
          new Object[]{testNameToTime.getSecond(), testNameToTime.getFirst()});
    }
  }

  private long getRandomLongKey(final Random random) {
    long key = random.nextLong() % maxDataKey;
    while (key < 0) {
      key = random.nextLong() % maxDataKey;
    }
    return key;
  }

  private Pair<Long, Long> getRandomLongRangeKey(final Random random, final int length) {
    final long randomKey = getRandomLongKey(random);
    long startKey = randomKey - (length - 1);
    if (startKey < 0) {
      startKey = startKey + length;
    }
    final long endKey = startKey + (length - 1);

    return new Pair<>(startKey, endKey);
  }

  private interface Test {
    void run() throws InterruptedException, IdGenerationException;
  }

  /**
   * A test that puts the data of random keys through one memory store.
   * The keys are naturally distributed to global memory stores.
   * Check that all the data are correctly put via remote access.
   */
  private class TestRandomPutGetRemove implements Test {

    @Override
    public void run() throws InterruptedException, IdGenerationException {

      final int numItems = 10000;
      Map<Long, Integer> outputMap;

      outputMap = memoryStore.getRange(DATA_TYPE, 0L, maxDataKey);
      if (!outputMap.isEmpty()) {
        throw new RuntimeException("Wrong initial state");
      }

      synchronize();

      if (localMemoryStoreId == 0) {
        final Random random = new Random();

        final Set<Long> keySet = new HashSet<>();
        while (keySet.size() < numItems) {
          final long key = getRandomLongKey(random);
          keySet.add(key);
        }

        final List<Long> keyList = new ArrayList<>(keySet);

        final Map<Long, Boolean> putResult = memoryStore.putList(DATA_TYPE, keyList, keyList);
        for (final Boolean value : putResult.values()) {
          if (!value) {
            throw new RuntimeException("Fail to put data");
          }
        }
      }

      synchronize();

      // check that the total number of objects equal the expected number
      final int numLocalData = memoryStore.getNumUnits(DATA_TYPE);
      final long numGlobalData = syncGlobalCount(numLocalData);
      LOG.log(Level.FINE, "numLocalData: {0}, numGlobalData: {1}", new Object[]{numLocalData, numGlobalData});
      if (numGlobalData != numItems) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }

      synchronize();

      outputMap = memoryStore.removeRange(DATA_TYPE, 0L, maxDataKey);

      final long numGlobalRemoves = syncGlobalCount(outputMap.size());
      LOG.log(Level.INFO, "localRemoves: {0}, numGlobalRemoves: {1}", new Object[]{outputMap.size(), numGlobalRemoves});
      if (numGlobalRemoves != numItems) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      // check that the total number of objects equal the expected number
      outputMap = memoryStore.getRange(DATA_TYPE, 0L, maxDataKey);
      LOG.log(Level.FINE, "outputMap.size: {0}", new Object[]{outputMap.size()});
      if (outputMap.size() != 0) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put single objects concurrently.
   */
  private class TestMultiThreadRemotePutSingle implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int putsPerThread = 2000;
      final int totalNumberOfObjects = numThreads * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads];
      for (int index = 0; index < numThreads; index++) {
        threads[index] = new PutThread(putsPerThread, 1, remoteIdFactory);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[index].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }
      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      synchronize();
      final int numUnits = memoryStore.getNumUnits(DATA_TYPE);

      // check that the total number of objects equal the expected number
      if (numUnits != totalNumberOfObjects) {
        throw new RuntimeException(MSG_LOCAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put a range of objects concurrently.
   */
  private class TestMultiThreadRemotePutRange implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int itemsPerPut = 10;
      final int putsPerThread = 1000;
      final int totalNumberOfObjects = numThreads * itemsPerPut * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads];
      for (int index = 0; index < numThreads; index++) {
        threads[index] = new PutThread(putsPerThread, itemsPerPut, remoteIdFactory);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[index].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      synchronize();
      final int numUnits = memoryStore.getNumUnits(DATA_TYPE);

      // check that the total number of objects equal the expected number
      if (numUnits != totalNumberOfObjects) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} and {@code get} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put single objects concurrently.
   */
  private class TestMultiThreadRemotePutGetSingle implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int putsPerThread = 2000;
      final int getsPerThread = 2000;
      final int totalNumberOfObjects = numThreads * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads * 2];
      for (int index = 0; index < numThreads; index++) {
        threads[2 * index] = new PutThread(putsPerThread, 1, remoteIdFactory);
        threads[2 * index + 1] = new GetThread(getsPerThread, 1);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      long numTotalGetSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[2 * index].get();
          numTotalGetSuccess += futures[2 * index + 1].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      LOG.log(Level.FINE, "numTotalPutSuccess: {0}", numTotalPutSuccess);
      LOG.log(Level.FINE, "numTotalGetSuccess: {0}", numTotalGetSuccess);

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      synchronize();
      final int numUnits = memoryStore.getNumUnits(DATA_TYPE);

      // check that the total number of objects equal the expected number
      if (numUnits != totalNumberOfObjects) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} and {@code get} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put a range of objects concurrently.
   */
  private class TestMultiThreadRemotePutGetRange implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int itemsPerPut = 10;
      final int itemsPerGet = 10;
      final int putsPerThread = 1000;
      final int getsPerThread = 1000;
      final int totalNumberOfObjects = numThreads * itemsPerPut * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads * 2];
      for (int index = 0; index < numThreads; index++) {
        threads[2 * index] = new PutThread(putsPerThread, itemsPerPut, remoteIdFactory);
        threads[2 * index + 1] = new GetThread(getsPerThread, itemsPerGet);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      long numTotalGetSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[2 * index].get();
          numTotalGetSuccess += futures[2 * index + 1].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      LOG.log(Level.FINE, "numTotalPutSuccess: {0}", numTotalPutSuccess);
      LOG.log(Level.FINE, "numTotalGetSuccess: {0}", numTotalGetSuccess);

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      synchronize();
      final int numUnits = memoryStore.getNumUnits(DATA_TYPE);

      // check that the total number of objects equal the expected number
      if (numUnits != totalNumberOfObjects) {
        throw new RuntimeException(MSG_LOCAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put}, {@code get}, and {@code remove} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put single objects concurrently.
   */
  private class TestMultiThreadRemotePutGetRemoveSingle implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int putsPerThread = 3000;
      final int getsPerThread = 3000;
      final int removesPerThread = 3000;
      final int totalNumberOfObjects = numThreads * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads * 3];
      for (int index = 0; index < numThreads; index++) {
        threads[3 * index] = new PutThread(putsPerThread, 1, remoteIdFactory);
        threads[3 * index + 1] = new GetThread(getsPerThread, 1);
        threads[3 * index + 2] = new RemoveThread(removesPerThread, 1);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      long numTotalGetSuccess = 0;
      long numTotalRemoveSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[3 * index].get();
          numTotalGetSuccess += futures[3 * index + 1].get();
          numTotalRemoveSuccess += futures[3 * index + 2].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      LOG.log(Level.FINE, "numTotalPutSuccess: {0}", numTotalPutSuccess);
      LOG.log(Level.FINE, "numTotalGetSuccess: {0}", numTotalGetSuccess);
      LOG.log(Level.FINE, "numTotalRemoveSuccess: {0}", numTotalRemoveSuccess);

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      final long numGlobalRemoves = syncGlobalCount(numTotalRemoveSuccess);

      LOG.log(Level.FINE, "numGlobalRemoves: {0}", numGlobalRemoves);

      synchronize();

      // check that the total number of objects equal the expected number
      final int numLocalData = memoryStore.getNumUnits(DATA_TYPE);
      final long numGlobalData = syncGlobalCount(numLocalData);
      LOG.log(Level.FINE, "numLocalData: {0}, numGlobalData: {1}", new Object[]{numLocalData, numGlobalData});
      if (numGlobalData != totalNumberOfObjects * RemoteEMDriver.EVAL_NUM - numGlobalRemoves) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put}, {@code get}, and {@code remove} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * Also check that the consistency of the store is preserved
   * when multiple threads try to put a range of objects concurrently.
   */
  private class TestMultiThreadRemotePutGetRemoveRange implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int itemsPerPut = 10;
      final int itemsPerGet = 10;
      final int itemsPerRemove = 10;
      final int putsPerThread = 1000;
      final int getsPerThread = 1000;
      final int removesPerThread = 1000;
      final int totalNumberOfObjects = numThreads * itemsPerPut * putsPerThread;
      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      final Callable[] threads = new Callable[numThreads * 3];
      for (int index = 0; index < numThreads; index++) {
        threads[3 * index] = new PutThread(putsPerThread, itemsPerPut, remoteIdFactory);
        threads[3 * index + 1] = new GetThread(getsPerThread, itemsPerGet);
        threads[3 * index + 2] = new RemoveThread(removesPerThread, itemsPerRemove);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      long numTotalGetSuccess = 0;
      long numTotalRemoveSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[3 * index].get();
          numTotalGetSuccess += futures[3 * index + 1].get();
          numTotalRemoveSuccess += futures[3 * index + 2].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      LOG.log(Level.FINE, "numTotalPutSuccess: {0}", numTotalPutSuccess);
      LOG.log(Level.FINE, "numTotalGetSuccess: {0}", numTotalGetSuccess);
      LOG.log(Level.FINE, "numTotalRemoveSuccess: {0}", numTotalRemoveSuccess);

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      final long numGlobalRemoves = syncGlobalCount(numTotalRemoveSuccess);

      LOG.log(Level.FINE, "numGlobalRemoves: {0}", numGlobalRemoves);

      synchronize();

      // check that the total number of objects equal the expected number
      final int numLocalData = memoryStore.getNumUnits(DATA_TYPE);
      final long numGlobalData = syncGlobalCount(numLocalData);
      LOG.log(Level.FINE, "numLocalData: {0}, numGlobalData: {1}", new Object[]{numLocalData, numGlobalData});
      if (numGlobalData != totalNumberOfObjects * RemoteEMDriver.EVAL_NUM - numGlobalRemoves) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} without remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * The purpose of the test is to check that DataIdFactory works correctly, issuing local data keys.
   */
  private class TestMultiThreadLocalPut implements Test {

    @Override
    public void run() throws InterruptedException, IdGenerationException {
      final Random random = new Random();

      final int numThreads = 8;
      final int itemsPerPut = random.nextInt(10) + 10;
      final int putsPerThread = random.nextInt(10000) + 10000;
      final int totalNumberOfObjects = numThreads * itemsPerPut * putsPerThread;

      final Callable[] threads = new Callable[numThreads];
      for (int index = 0; index < numThreads; index++) {
        threads[index] = new PutThread(putsPerThread, itemsPerPut, localDataIdFactory);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalPutSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalPutSuccess += futures[index].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      if (numTotalPutSuccess != totalNumberOfObjects) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      synchronize();
      final Map<Long, Object> outputMap = memoryStore.getAll(DATA_TYPE);

      LOG.log(Level.FINE, "outputMap.size(): {0}", outputMap.size());

      // check that the total number of objects equal the expected number
      if (outputMap.size() != totalNumberOfObjects) {
        throw new RuntimeException(MSG_LOCAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * Multithreading test for {@code put} involving remote access.
   * Checks that the all the operations by multiple threads are performed successfully.
   * At the end of the test, stores should have different number of local data, based on its own store id.
   */
  private class TestMultiThreadRelayedPutSingle implements Test {

    @Override
    public void run() throws InterruptedException {
      final int numThreads = 8;
      final int putsPerThread = 5000;
      final int remotePutsPerThread = putsPerThread + localMemoryStoreId; // different number for each store
      final int totalNumberOfObjectsPutByLocal = numThreads * (putsPerThread + remotePutsPerThread);

      final DataIdFactory<Long> remoteIdFactory = initDataIdFactory(nextRemoteStoreId);

      // consume ids that overlaps with ids that will be used by the remote store
      try {
        remoteIdFactory.getIds(numThreads * putsPerThread);
      } catch (final IdGenerationException e) {
        throw new RuntimeException(e);
      }

      final Callable[] threads = new Callable[numThreads * 2];
      for (int index = 0; index < numThreads; index++) {
        threads[2 * index] = new PutThread(remotePutsPerThread, 1, remoteIdFactory);
        threads[2 * index + 1] = new PutThread(putsPerThread, 1, localDataIdFactory);
      }
      final Future<Long>[] futures = ThreadUtils.runConcurrentlyWithResult(threads);

      long numTotalRemotePutSuccess = 0;
      long numTotalLocalPutSuccess = 0;
      // check that all threads have finished successfully without falling into deadlocks or infinite loops
      for (int index = 0; index < numThreads; index++) {
        try {
          numTotalRemotePutSuccess += futures[2 * index].get();
          numTotalLocalPutSuccess += futures[2 * index + 1].get();
        } catch (final ExecutionException e) {
          LOG.log(Level.SEVERE, "Test thread failed", e);
        }
      }

      if (numTotalRemotePutSuccess + numTotalLocalPutSuccess != totalNumberOfObjectsPutByLocal) {
        throw new RuntimeException(MSG_OPERATION_FAILED);
      }

      // wait put operations of whole stores are finished
      synchronize();
      final int totalNumberOfLocalObjects = numThreads * putsPerThread +
          numThreads * (putsPerThread + prevRemoteStoreId);

      if (memoryStore.getAll(DATA_TYPE).size() != totalNumberOfLocalObjects) {
        throw new RuntimeException(MSG_LOCAL_SIZE_ASSERTION);
      }

      int expectedNumGlobalData = 0;
      for (int memStoreId = 0; memStoreId < numInitialEvals; memStoreId++) {
        expectedNumGlobalData += numThreads * (putsPerThread + putsPerThread + memStoreId);
      }

      // check that the total number of objects equal the expected number
      final int numLocalData = memoryStore.getNumUnits(DATA_TYPE);
      final long numGlobalData = syncGlobalCount(numLocalData);
      LOG.log(Level.FINE, "numLocalData: {0}, numGlobalData: {1}", new Object[]{numLocalData, numGlobalData});
      if (numGlobalData != expectedNumGlobalData) {
        throw new RuntimeException(MSG_GLOBAL_SIZE_ASSERTION);
      }
    }
  }

  /**
   * A thread executing put operation on data keys from the given DataIdFactory.
   * It returns the number of succeeded put operations.
   */
  private class PutThread implements Callable<Long> {

    private int putsPerThread;
    private int itemsPerPut;
    private DataIdFactory<Long> idFactory;

    PutThread(final int putsPerThread, final int itemsPerPut, final DataIdFactory<Long> idFactory) {
      this.putsPerThread = putsPerThread;
      this.itemsPerPut = itemsPerPut;
      this.idFactory = idFactory;
    }

    @Override
    public Long call() throws Exception {
      long numPutSuccess = 0;

      for (int i = 0; i < putsPerThread; i++) {
        if (itemsPerPut == 1) {
          final long key = idFactory.getId();
          final Pair<Long, Boolean> res = memoryStore.put(DATA_TYPE, key, key);

          if (res.getSecond()) {
            numPutSuccess++;
          }
        } else {
          final List<Long> keys = idFactory.getIds(itemsPerPut);
          final Map<Long, Boolean> res = memoryStore.putList(DATA_TYPE, keys, keys);

          for (final Boolean value : res.values()) {
            if (value) {
              numPutSuccess++;
            }
          }
        }
      }
      return numPutSuccess;
    }
  }

  /**
   * A thread executing get operation on randomly chosen data keys.
   * It returns the number of succeeded get operations.
   */
  private class GetThread implements Callable<Long> {

    private int getsPerThread;
    private int itemsPerGet;

    GetThread(final int getsPerThread, final int itemsPerGet) {
      this.getsPerThread = getsPerThread;
      this.itemsPerGet = itemsPerGet;
    }

    @Override
    public Long call() throws Exception {
      long numGetSuccess = 0;

      final Random random = new Random();

      for (int i = 0; i < getsPerThread; i++) {
        if (itemsPerGet == 1) {
          final long key = getRandomLongKey(random);
          final Pair<Long, Object> res = memoryStore.get(DATA_TYPE, key);

          if (res != null) {
            numGetSuccess++;
          }
        } else {
          final Pair<Long, Long> range = getRandomLongRangeKey(random, itemsPerGet);
          final Map<Long, Object> res = memoryStore.getRange(DATA_TYPE, range.getFirst(), range.getSecond());

          numGetSuccess += res.size();
        }
      }
      return numGetSuccess;
    }
  }

  /**
   * A thread executing remove operation on randomly chosen data keys.
   * It returns the number of succeeded remove operations.
   */
  private class RemoveThread implements Callable<Long> {

    private int removesPerThread;
    private int itemsPerRemove;

    RemoveThread(final int removesPerThread, final int itemsPerRemove) {
      this.removesPerThread = removesPerThread;
      this.itemsPerRemove = itemsPerRemove;
    }

    @Override
    public Long call() throws Exception {
      long numRemoveSuccess = 0;

      final Random random = new Random();

      for (int i = 0; i < removesPerThread; i++) {
        if (itemsPerRemove == 1) {
          final long key = getRandomLongKey(random);
          final Pair<Long, Object> res = memoryStore.remove(DATA_TYPE, key);

          if (res != null) {
            numRemoveSuccess++;
          }
        } else {
          final Pair<Long, Long> range = getRandomLongRangeKey(random, itemsPerRemove);
          final Map<Long, Object> res = memoryStore.removeRange(DATA_TYPE, range.getFirst(), range.getSecond());

          numRemoveSuccess += res.size();
        }
      }
      return numRemoveSuccess;
    }
  }

  /**
   * A test with a simple scenario that accesses the store in a single thread, using only two data keys.
   * Put and remove operations executed via remote access, changing the state of stores.
   * The changes are happen in separate steps with a global synchronized barrier.
   * At each step, all stores invoke get operation to confirm that the own state is correct.
   */
  private class TestSimpleScenario implements Test {

    public void run() throws IdGenerationException {

      final long dataKey0 = 0;
      final long dataKey1 = 1;
      final List<Long> keys = new ArrayList<>(2);
      keys.add(dataKey0);
      keys.add(dataKey1);


      final int dataValue0 = 1000;
      final int dataValue1 = 1001;
      final List<Integer> values = new ArrayList<>(2);
      values.add(dataValue0);
      values.add(dataValue1);

      Pair<Long, Integer> outputPair;
      Map<Long, Integer> outputMap;

      final List<Pair<Long, Long>> rangeList = new ArrayList<>(1);
      rangeList.add(new Pair<>(0L, 1L));
      final boolean isLocalKey = !router.route(rangeList).getFirst().isEmpty();

      // 1. INITIAL STATE: check that the store does not contain DATA
      outputMap = memoryStore.getRange(DATA_TYPE, dataKey0, dataKey1);
      LOG.log(Level.FINE, "getRange({0}, {1}): {2}", new Object[]{dataKey0, dataKey1, outputMap});

      if (!outputMap.isEmpty()) {
        throw new RuntimeException("Wrong initial state");
      }

      synchronize();

      // 2. Put DATA into store via remote access
      // It should be performed by a memory store that does not own DATA_KEY.
      if (!isLocalKey) {
        final Map<Long, Boolean> putResult = memoryStore.putList(DATA_TYPE, keys, values);

        LOG.log(Level.FINE, "putList({0}, {1}): {2}", new Object[]{keys, values, putResult});
        for (final Boolean value : putResult.values()) {
          if (!value) {
            throw new RuntimeException("Fail to put data");
          }
        }
      }

      synchronize();

      // 3. AFTER PUT: check that all workers can get DATA from the store
      outputPair = memoryStore.get(DATA_TYPE, dataKey0);
      LOG.log(Level.FINE, "get({0}): {1}", new Object[]{dataKey0, outputPair});

      if (outputPair == null) {
        throw new RuntimeException("Fail to get data");
      }
      if (outputPair.getFirst() != dataKey0 || outputPair.getSecond() != dataValue0) {
        throw new RuntimeException("Fail to get correct data");
      }

      outputPair = memoryStore.get(DATA_TYPE, dataKey1);
      LOG.log(Level.FINE, "get({0}): {1}", new Object[]{dataKey1, outputPair});

      if (outputPair == null) {
        throw new RuntimeException("Fail to get data");
      }
      if (outputPair.getFirst() != dataKey1 || outputPair.getSecond() != dataValue1) {
        throw new RuntimeException("Fail to get correct data");
      }

      outputMap = memoryStore.getRange(DATA_TYPE, 0L, 1L);
      LOG.log(Level.FINE, "getRange({0}, {1}): {2}", new Object[]{dataKey0, dataKey1, outputMap});

      if (!outputMap.containsKey(dataKey0) || !outputMap.containsKey(dataKey1)) {
        throw new RuntimeException("Fail to get data");
      }
      if (!outputMap.get(dataKey0).equals(dataValue0) || !outputMap.get(dataKey1).equals(dataValue1)) {
        throw new RuntimeException("Fail to get correct data");
      }

      synchronize();

      // 4. Remove DATA from the store via remote access
      // It should be performed by a memory store that does not own DATA_KEY.
      if (!isLocalKey) {
        outputMap = memoryStore.removeRange(DATA_TYPE, dataKey0, dataKey1);
        LOG.log(Level.FINE, "removeRange({0}, {1}): {2}", new Object[]{dataKey0, dataKey1, outputMap});

        if (!outputMap.containsKey(dataKey0) || !outputMap.containsKey(dataKey1)) {
          throw new RuntimeException("Fail to remove data");
        }
        if (!outputMap.get(dataKey0).equals(dataValue0) || !outputMap.get(dataKey1).equals(dataValue1)) {
          throw new RuntimeException("Fail to remove correct data");
        }
      }

      synchronize();

      // 5. AFTER REMOVE: check that the store does not contain DATA
      outputMap = memoryStore.getRange(DATA_TYPE, dataKey0, dataKey1);
      LOG.log(Level.FINE, "getRange({0}, {1}): {2}", new Object[]{dataKey0, dataKey1, outputMap});

      if (!outputMap.isEmpty()) {
        throw new RuntimeException("RemoveRange did not work");
      }
    }
  }
}
