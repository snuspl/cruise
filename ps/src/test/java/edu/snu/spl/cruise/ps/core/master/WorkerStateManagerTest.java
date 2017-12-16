/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.ps.core.master;

import edu.snu.spl.cruise.ps.PSMsg;
import edu.snu.spl.cruise.ps.CruisePSParameters;
import edu.snu.spl.cruise.ps.SyncMsg;
import edu.snu.spl.cruise.ps.core.worker.*;
import edu.snu.spl.cruise.ps.cruiseMsgType;
import edu.snu.spl.cruise.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.spl.cruise.services.et.driver.impl.RunningTasklet;
import edu.snu.spl.cruise.services.et.evaluator.api.TaskletCustomMsgHandler;
import edu.snu.spl.cruise.utils.ThreadUtils;
import edu.snu.spl.cruise.utils.Tuple3;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkerStateManager} and {@link WorkerGlobalBarrier}.
 * It tests whether worker states are managed correctly,
 * and workers are synchronized correctly during their lifecycle (INIT -> RUN -> CLEANUP).
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerSideMsgSender.class, ETTaskRunner.class, RunningTasklet.class,
    ProgressTracker.class, BatchProgressTracker.class, ModelChkpManager.class, ModelEvaluator.class})
public class WorkerStateManagerTest {
  private static final Logger LOG = Logger.getLogger(WorkerStateManagerTest.class.getName());
  private static final String JOB_ID = WorkerStateManagerTest.class.getName();
  private static final String DRIVER_ID = "DRIVER";

  private static final String WORKER_ID_PREFIX = "worker-";
  private static final long SYNC_WAIT_TIME_MS = 1000;

  private static final String MSG_SHOULD_WAIT_OTHER_WORKERS =
      "Cannot enter next state before all workers reach the same global barrier";
  private static final String MSG_SHOULD_RELEASE_WORKERS = "All workers should be released";

  private Pair<WorkerStateManager, MasterSideMsgHandler> driverComponents;

  private Map<String, Tuple3<WorkerGlobalBarrier, WorkerSideMsgSender, TaskletCustomMsgHandler>>
      workerIdToWorkerComponents = new HashMap<>();

  @Before
  public void setup() {
    driverComponents = null;
    workerIdToWorkerComponents.clear();
  }

  /**
   * Set up the Driver. The mocked message handler for CentComm service pretends the actual messages to be exchanged.
   */
  private void setupDriver(final List<String> workerIds) throws InjectionException, NetworkException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(CruisePSParameters.NumWorkers.class, workerIds.size());
    injector.bindVolatileParameter(JobIdentifier.class, JOB_ID);
    injector.bindVolatileParameter(DriverIdentifier.class, DRIVER_ID);

    // mock classes that this test does not use
    injector.bindVolatileInstance(ProgressTracker.class, mock(ProgressTracker.class));
    injector.bindVolatileInstance(BatchProgressTracker.class, mock(BatchProgressTracker.class));
    injector.bindVolatileInstance(ModelChkpManager.class, mock(ModelChkpManager.class));
    injector.bindVolatileInstance(JobMessageObserver.class, mock(JobMessageObserver.class));
    injector.bindVolatileInstance(EvaluatorRequestor.class, mock(EvaluatorRequestor.class));
    final ETTaskRunner mockedTaskRunner = mock(ETTaskRunner.class);
    injector.bindVolatileInstance(ETTaskRunner.class, mockedTaskRunner);

    final Map<String, RunningTasklet> runningTaskletMap = new HashMap<>();
    workerIds.forEach(workerId -> {
      final RunningTasklet mockedTasklet = mock(RunningTasklet.class);
      try {
        doAnswer(invocation -> {
          LOG.log(Level.INFO, "sending a release msg to {0}", workerId);
          final byte[] bytes = invocation.getArgumentAt(0, byte[].class);
          workerIdToWorkerComponents.get(workerId).getThird().onNext(bytes);
          return null;
        }).when(mockedTasklet).send(any(byte[].class));
      } catch (NetworkException e) {
        throw new RuntimeException(e);
      }

      runningTaskletMap.put(workerId, mockedTasklet);
    });

    doAnswer(invocation -> {
      final String workerId = invocation.getArgumentAt(0, String.class);
      return runningTaskletMap.get(workerId);
    }).when(mockedTaskRunner).getRunningTasklet(anyString());

    final WorkerStateManager workerStateManager = injector.getInstance(WorkerStateManager.class);

    final MasterSideMsgHandler masterSideMsgHandler = injector.getInstance(MasterSideMsgHandler.class);

    driverComponents = Pair.of(workerStateManager, masterSideMsgHandler);
  }

  /**
   * Set up a worker. The mocked message handler for CentComm service pretends the actual messages to be exchanged.
   * This method should be called after {@link #setupDriver(List)}.
   * @param workerId a worker id
   */
  private void setupWorker(final String workerId) throws InjectionException, NetworkException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(ExecutorIdentifier.class, workerId);
    injector.bindVolatileParameter(JobIdentifier.class, JOB_ID);
    injector.bindVolatileParameter(DriverIdentifier.class, DRIVER_ID);

    // mock classes that this test does not use
    injector.bindVolatileInstance(ModelEvaluator.class, mock(ModelEvaluator.class));

    final WorkerSideMsgSender mockedWorkerSideMsgSender = mock(WorkerSideMsgSender.class);
    injector.bindVolatileInstance(WorkerSideMsgSender.class, mockedWorkerSideMsgSender);

    final WorkerGlobalBarrier workerGlobalBarrier = injector.getInstance(WorkerGlobalBarrier.class);
    final TaskletCustomMsgHandler taskletCustomMsgHandler = injector.getInstance(WorkerSideMsgHandler.class);
    final SerializableCodec<WorkerGlobalBarrier.State> codec = new SerializableCodec<>();

    workerIdToWorkerComponents.put(workerId,
        new Tuple3<>(workerGlobalBarrier, mockedWorkerSideMsgSender, taskletCustomMsgHandler));

    doAnswer(invocation -> {
      final WorkerGlobalBarrier.State state = invocation.getArgumentAt(0, WorkerGlobalBarrier.State.class);
      final PSMsg cruiseMsg = PSMsg.newBuilder()
          .setJobId(JOB_ID)
          .setType(cruiseMsgType.SyncMsg)
          .setSyncMsg(
              SyncMsg.newBuilder()
              .setExecutorId(workerId)
              .setSerializedState(ByteBuffer.wrap(codec.encode(state)))
              .build()
          )
          .build();

      LOG.log(Level.INFO, "sending a progress msg from {0}", workerId);
      final MasterSideMsgHandler masterSideMsgHandler = driverComponents.getRight();
      masterSideMsgHandler.onPSMsg(cruiseMsg);
      return null;
    }).when(mockedWorkerSideMsgSender).sendSyncMsg(any(WorkerGlobalBarrier.State.class));
  }

  /**
   * Prepare {@link WorkerStateManager}-related components of driver and workers.
   * @param numInitialWorkers the number of initial workers
   * @return a list of worker ids
   */
  private List<String> prepare(final int numInitialWorkers) throws NetworkException, InjectionException {
    final List<String> workerIds = new ArrayList<>(numInitialWorkers);
    for (int workerIdx = 0; workerIdx < numInitialWorkers; workerIdx++) {
      final String workerId = WORKER_ID_PREFIX + workerIdx;
      setupWorker(workerId);
      workerIds.add(workerId);
    }
    setupDriver(workerIds);
    return workerIds;
  }

  @Test
  public void testStageSync() throws InjectionException, NetworkException, InterruptedException, ExecutionException {
    final int numWorkers = 3;
    final List<String> workerIds = prepare(numWorkers);

    // 1. INIT -> RUN : all workers should be synchronized
    final CountDownLatch firstLatch = callGlobalBarrier(workerIds.get(0), workerIds.get(1));
    assertFalse(MSG_SHOULD_WAIT_OTHER_WORKERS, // should wait worker-2, thus await() returns false
        firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    assertEquals(MSG_SHOULD_WAIT_OTHER_WORKERS, 2, firstLatch.getCount()); // worker-0, worker-1 are waiting

    final CountDownLatch firstLatchAllWorkers = callGlobalBarrier(workerIds.get(2));
    assertTrue(MSG_SHOULD_RELEASE_WORKERS, firstLatchAllWorkers.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    assertTrue(MSG_SHOULD_RELEASE_WORKERS, firstLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));

    // 2. RUN -> CLEANUP : all workers should be synchronized and driver should allow them to enter the next state
    final CountDownLatch secondLatch = callGlobalBarrier(workerIds.get(1), workerIds.get(2));
    assertFalse(MSG_SHOULD_WAIT_OTHER_WORKERS, // should wait worker-0, thus await() returns false
        secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    assertEquals(MSG_SHOULD_WAIT_OTHER_WORKERS, 2, secondLatch.getCount()); // worker-1, worker-2 are waiting

    final CountDownLatch secondLatchAllWorkers = callGlobalBarrier(workerIds.get(0));
    assertTrue(MSG_SHOULD_RELEASE_WORKERS, secondLatchAllWorkers.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
    assertTrue(MSG_SHOULD_RELEASE_WORKERS, secondLatch.await(SYNC_WAIT_TIME_MS, TimeUnit.MILLISECONDS));
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
      final WorkerGlobalBarrier workerGlobalBarrier = workerIdToWorkerComponents.get(workerId).getFirst();
      try {
        workerGlobalBarrier.await();
      } catch (NetworkException e) {
        throw new RuntimeException(e);
      }
      latch.countDown();
    }
  }

  /**
   * Requests a global barrier to synchronize workers.
   * @param workerIds a set of worker ids to be synchronized
   * @return a latch that indicates whether the workers passed the global barrier.
   */
  private CountDownLatch callGlobalBarrier(final String ... workerIds) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(workerIds.length);

    final Runnable[] threads = new Runnable[workerIds.length];
    for (int i = 0; i < workerIds.length; i++) {
      threads[i] = new WorkerThread(workerIds[i], latch);
    }
    ThreadUtils.runConcurrently(threads);
    return latch;
  }
}
