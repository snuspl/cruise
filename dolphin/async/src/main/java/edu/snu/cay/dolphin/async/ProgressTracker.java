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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.utils.StateMachine;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.ProgressProvider;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that tracks epoch progress of running workers, maintaining global minimum epoch progress.
 * It also provides progress info to REEF by implementing {@link ProgressProvider}
 * and to a client through {@link JobMessageObserver}.
 */
@DriverSide
@ThreadSafe
public final class ProgressTracker implements ProgressProvider {
  private static final Logger LOG = Logger.getLogger(ProgressTracker.class.getName());

  private final int maxNumEpochs;
  private final int numWorkers;
  private volatile int globalMinEpochIdx;

  private final JobMessageObserver jobMessageObserver;

  private final StateMachine stateMachine;

  private final Map<String, Integer> workerIdToEpochIdx = new ConcurrentHashMap<>();

  private final NavigableMap<Integer, Set<String>> epochProgressToWorkerIds = new ConcurrentSkipListMap<>();

  @Inject
  private ProgressTracker(final JobMessageObserver jobMessageObserver,
                          @Parameter(DolphinParameters.MaxNumEpochs.class) final int maxNumEpochs,
                          @Parameter(DolphinParameters.NumWorkers.class) final int numWorkers) {
    this.jobMessageObserver = jobMessageObserver;
    this.maxNumEpochs = maxNumEpochs;
    this.numWorkers = numWorkers;
    this.globalMinEpochIdx = 0;
    this.stateMachine = initStateMachine();
  }

  @Override
  public float getProgress() {
    return ((float) getGlobalMinEpochIdx()) / maxNumEpochs;
  }

  private enum State {
    INIT,
    RUN
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(State.INIT, "Not all initial workers start training")
        .addState(State.RUN, "All initial workers has started training")
        .addTransition(State.INIT, State.RUN, "All initial workers has started training.")
        .setInitialState(State.INIT)
        .build();
  }

  /**
   * @return a map between worker id and its epoch progress
   */
  public Map<String, Integer> getWorkerIdToEpochIdx() {
    return new HashMap<>(workerIdToEpochIdx);
  }

  /**
   * @return a global minimum epoch progress
   */
  public int getGlobalMinEpochIdx() {
    return stateMachine.getCurrentState().equals(State.INIT) ? 0 : globalMinEpochIdx;
  }

  /**
   * Updates global minimum epoch index and sends progress info to client.
   * @param newMinEpochIdx a new epoch index
   */
  private void updateGlobalMinEpochIdx(final int newMinEpochIdx) {
    globalMinEpochIdx = newMinEpochIdx;
    final String msgToClient = String.format("Epoch progress is [%d / %d]", newMinEpochIdx, maxNumEpochs);
    jobMessageObserver.sendMessageToClient(msgToClient.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Handles progress messages from workers.
   * {@link ProgressTracker} is thread-safe, because this method is synchronized
   * and all mutable states of {@link ProgressTracker} are updated in this method.
   */
  synchronized void onProgressMsg(final ProgressMsg progressMsg) {
    final String workerId = progressMsg.getExecutorId().toString();
    final int epochProgress = progressMsg.getEpochIdx();
    LOG.log(Level.INFO, "Epoch progress reported by {0}: {1}", new Object[]{workerId, epochProgress});

    final Integer prevEpochProgress = workerIdToEpochIdx.put(workerId, epochProgress);

    if (prevEpochProgress != null) {
      epochProgressToWorkerIds.compute(prevEpochProgress, (k, v) -> {
        v.remove(workerId);
        return v.isEmpty() ? null : v;
      });
    }

    epochProgressToWorkerIds.compute(epochProgress, (k, v) -> {
      final Set<String> workers = v == null ? Collections.newSetFromMap(new ConcurrentHashMap<>()) : v;
      workers.add(workerId);
      return workers;
    });

    if (stateMachine.getCurrentState().equals(State.INIT)) {
      if (workerIdToEpochIdx.size() == numWorkers) {
        LOG.log(Level.INFO, "State Transition: {0} -> {1}", new Object[]{State.INIT, State.RUN});
        stateMachine.setState(State.RUN);
        updateGlobalMinEpochIdx(0);
      }
    }

    final int newMinEpochIdx = epochProgressToWorkerIds.firstKey();
    if (newMinEpochIdx > globalMinEpochIdx) {
      updateGlobalMinEpochIdx(newMinEpochIdx);
    }
  }
}
