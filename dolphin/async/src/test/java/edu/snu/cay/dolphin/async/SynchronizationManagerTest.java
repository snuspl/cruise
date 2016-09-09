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

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.utils.ThreadUtils;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link SynchronizationManager}.
 * It tests whether workers are synchronized correctly during their lifecycle (INIT -> RUN -> CLEANUP)
 * regarding to EM's worker Add/Delete.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AggregationMaster.class, AggregationSlave.class})
public class SynchronizationManagerTest {
  private static final String WORKER_ID_PREFIX = "worker";
  private static final long SYNC_WAIT_TIME_MS = 1000;

  private Tuple3<SynchronizationManager, AggregationMaster, EventHandler<AggregationMessage>> driverComponents;

  private Map<String, Tuple3<WorkerSynchronizer, AggregationSlave, EventHandler<AggregationMessage>>>
      workerIdToWorkerComponents = new HashMap<>();

  @Before
  public void setup() {
    driverComponents = null;
    workerIdToWorkerComponents.clear();
  }

  /**
   * Set up driver.
   * @param numInitialWorkers the number of initial workers
   * @throws InjectionException
   * @throws NetworkException
   */
  private void setupDriver(final int numInitialWorkers) throws InjectionException, NetworkException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    final AggregationMaster mockedAggregationMaster = mock(AggregationMaster.class);
    injector.bindVolatileInstance(AggregationMaster.class, mockedAggregationMaster);

    final DataLoadingService mockedDataLoadingService = mock(DataLoadingService.class);
    injector.bindVolatileInstance(DataLoadingService.class, mockedDataLoadingService);
    when(mockedDataLoadingService.getNumberOfPartitions()).thenReturn(numInitialWorkers);

    final SynchronizationManager synchronizationManager = injector.getInstance(SynchronizationManager.class);
    final EventHandler<AggregationMessage> driverSideMsgHandler =
        injector.getInstance(SynchronizationManager.MessageHandler.class);

    driverComponents = new Tuple3<>(synchronizationManager, mockedAggregationMaster, driverSideMsgHandler);

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        final byte[] data = invocation.getArgumentAt(2, byte[].class);

        final AggregationMessage msg = AggregationMessage.newBuilder()
            .setSourceId("")
            .setClientClassName("")
            .setData(ByteBuffer.wrap(data))
            .build();

        final String workerId = invocation.getArgumentAt(1, String.class);
        final EventHandler<AggregationMessage> workerSideMsgHandler =
            workerIdToWorkerComponents.get(workerId).getThird();

        workerSideMsgHandler.onNext(msg);
        return null;
      }
    }).when(mockedAggregationMaster).send(anyString(), anyString(), any(byte[].class));
  }

  /**
   * Set up worker.
   * Should be invoked after {@link #setupDriver(int)}.
   * @param workerId an worker id
   * @throws InjectionException
   */
  private void setupWorker(final String workerId) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    final AggregationSlave mockedAggregationSlave = mock(AggregationSlave.class);
    injector.bindVolatileInstance(AggregationSlave.class, mockedAggregationSlave);

    final WorkerSynchronizer workerSynchronizer = injector.getInstance(WorkerSynchronizer.class);
    final EventHandler<AggregationMessage> workerSideMsgHandler =
        injector.getInstance(WorkerSynchronizer.MessageHandler.class);

    workerIdToWorkerComponents.put(workerId,
        new Tuple3<>(workerSynchronizer, mockedAggregationSlave, workerSideMsgHandler));

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocation) throws Throwable {
        final byte[] data = invocation.getArgumentAt(1, byte[].class);

        final AggregationMessage msg = AggregationMessage.newBuilder()
            .setSourceId(workerId)
            .setClientClassName("")
            .setData(ByteBuffer.wrap(data))
            .build();

        final EventHandler<AggregationMessage> driverSideMsgHandler = driverComponents.getThird();

        driverSideMsgHandler.onNext(msg);
        return null;
      }
    }).when(mockedAggregationSlave).send(anyString(), any(byte[].class));

    // should be done after setting up driver
    final SynchronizationManager synchronizationManager = driverComponents.getFirst();
    synchronizationManager.onWorkerAdded();
  }

  /**
   * Prepare {@link SynchronizationManager}-related components of driver and workers.
   * @param numInitialWorkers the number of initial workers
   * @return a list of worker ids
   * @throws NetworkException
   * @throws InjectionException
   */
  private List<String> prepare(final int numInitialWorkers) throws NetworkException, InjectionException {
    setupDriver(numInitialWorkers);
    final List<String> workerIds = new ArrayList<>(numInitialWorkers);
    for (int workerIdx = 0; workerIdx < numInitialWorkers; workerIdx++) {
      final String workerId = WORKER_ID_PREFIX + workerIdx;
      setupWorker(workerId);
      workerIds.add(workerId);
    }
    return workerIds;
  }

  @Test
  public void testSync() throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numWorkers = 3;

    final List<String> workerIds = prepare(numWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // 1. INIT -> RUN : all workers should be synchronized
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds.subList(0, 2));
    assertFalse("Cannot enter next state before all workers call global barrier",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    callGlobalBarrier(workerIds.get(2));
    assertTrue("All workers should be released after sync",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    // 2. RUN -> CLEANUP : all workers should be synchronized and driver should allow them to enter the next state
    final CountDownLatch secondLatch = callGlobalBarriers(workerIds.subList(1, 3));
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    callGlobalBarrier(workerIds.get(0));
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.allowWorkersCleanup();
    assertTrue("All workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testSyncWithOneDeleteBeforeCallingBarrier()
      throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numInitialWorkers = 3;

    final List<String> workerIds = prepare(numInitialWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // pass INIT barrier
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds);
    assertTrue("Workers should be released",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS)); // enter RUN state

    // one worker will be deleted without calling global barrier
    final CountDownLatch secondLatch = callGlobalBarriers(workerIds.subList(0, 2));
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.onWorkerDeleted(workerIds.get(2));
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.allowWorkersCleanup();
    assertTrue("Workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testSyncWithOneDeleteAfterCallingBarrier()
      throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numInitialWorkers = 3;

    final List<String> workerIds = prepare(numInitialWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // pass INIT barrier
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds);
    assertTrue("Workers should be released",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS)); // enter RUN state

    // one worker will be deleted after calling global barrier
    final CountDownLatch secondLatch = callGlobalBarriers(workerIds.subList(1, 3));
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.onWorkerDeleted(workerIds.get(2));
    secondLatch.countDown(); // count down latch on behalf of deleted worker
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    final Future future = callGlobalBarrier(workerIds.get(0));
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.allowWorkersCleanup();
    assertTrue("Workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    try {
      future.get(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      fail("Workers should be released");
    }
  }

  @Test
  public void testSyncWithOneAdd()
      throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numInitialWorkers = 3;

    final List<String> workerIds = prepare(numInitialWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // pass INIT barrier
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds);
    assertTrue("Workers should be released",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS)); // enter RUN state

    final String newWorkerId = WORKER_ID_PREFIX + 3;
    setupWorker(newWorkerId);

    final CountDownLatch secondLatch = callGlobalBarriers(workerIds);
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    try {
      callGlobalBarrier(newWorkerId).get(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS); // will pass first barrier directly
    } catch (final TimeoutException e) {
      fail("Workers should be released");
    }
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    callGlobalBarrier(newWorkerId); // will be synced with other workers
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.allowWorkersCleanup();
    assertTrue("Workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testSyncWithAddDuringCleanup()
      throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numInitialWorkers = 3;

    final List<String> workerIds = prepare(numInitialWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // pass INIT barrier
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds);
    assertTrue("Workers should be released",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS)); // enter RUN state

    final CountDownLatch secondLatch = callGlobalBarriers(workerIds);
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    // worker is added in CLEANUP state, it's possible because other workers can finish their task
    // when the optimization is ongoing, which adds a new worker.
    final String newWorkerId = WORKER_ID_PREFIX + 3;
    setupWorker(newWorkerId);

    synchronizationManager.allowWorkersCleanup();
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    try {
      callGlobalBarrier(newWorkerId).get(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS); // will pass first barrier directly
    } catch (final TimeoutException e) {
      fail("Workers should be released");
    }
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    callGlobalBarrier(newWorkerId);
    assertTrue("Workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testSyncWithAddAndDelete()
      throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numInitialWorkers = 3;

    final List<String> workerIds = prepare(numInitialWorkers);

    final SynchronizationManager synchronizationManager = driverComponents.getFirst();

    // pass INIT barrier
    final CountDownLatch firstLatch = callGlobalBarriers(workerIds);
    assertTrue("Workers should be released",
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS)); // enter RUN state

    final String newWorkerId = WORKER_ID_PREFIX + 3;
    setupWorker(newWorkerId);

    final CountDownLatch secondLatch = callGlobalBarriers(workerIds.subList(0, 2));
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.onWorkerDeleted(workerIds.get(1)); // deleted after calling barrier
    secondLatch.countDown(); // count down latch on behalf of deleted worker
    assertFalse("Cannot enter next state before all workers call the global barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.onWorkerDeleted(workerIds.get(2)); // deleted before calling barrier
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    try {
      callGlobalBarrier(newWorkerId).get(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS); // will pass first barrier directly
    } catch (final TimeoutException e) {
      fail("Workers should be released");
    }
    assertFalse("Cannot enter next state before the newly added worker reaches the same barrier",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    callGlobalBarrier(newWorkerId); // will be synced with other workers
    assertFalse("Cannot enter CLEANUP state before being allowed",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    synchronizationManager.allowWorkersCleanup();
    assertTrue("Workers should be released",
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
  }

  private final class WorkerThread implements Runnable {
    private final String workerId;
    private final CountDownLatch latch;

    WorkerThread(final String workerId, final CountDownLatch latch) {
      this.workerId = workerId;
      this.latch = latch;
    }

    @Override
    public void run() {
      final WorkerSynchronizer workerSynchronizer = workerIdToWorkerComponents.get(workerId).getFirst();
      workerSynchronizer.globalBarrier();
      latch.countDown();
    }
  }

  private CountDownLatch callGlobalBarriers(final List<String> workerIds) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(workerIds.size());

    final Runnable[] threads = new Runnable[workerIds.size()];
    for (int i = 0; i < workerIds.size(); i++) {
      threads[i] = new WorkerThread(workerIds.get(i), latch);
    }
    ThreadUtils.runConcurrently(threads);
    return latch;
  }

  private Future callGlobalBarrier(final String workerId) {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    return executorService.submit(new Runnable() {
      @Override
      public void run() {
        final WorkerSynchronizer workerSynchronizer = workerIdToWorkerComponents.get(workerId).getFirst();
        workerSynchronizer.globalBarrier();
      }
    });
  }
}
