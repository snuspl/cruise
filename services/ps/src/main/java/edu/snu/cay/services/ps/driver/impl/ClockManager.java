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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.avro.BroadcastMinClockMsg;
import edu.snu.cay.services.ps.avro.ClockMsgType;
import edu.snu.cay.services.ps.avro.ReplyInitClockMsg;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.parameters.Staleness;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A driver side component to manage worker clocks.
 * Receive all worker clocks and save the updated worker clock to the {@link ClockManager#workerClockMap}.
 * Broadcast the global minimum clock among all workers when it is changed.
 */
@DriverSide
@Unit
public final class ClockManager {
  public static final String AGGREGATION_CLIENT_NAME = ClockManager.class.getName();
  private static final Logger LOG = Logger.getLogger(ClockManager.class.getName());
  private static final int INITIAL_GLOBAL_MINIMUM_CLOCK = 0;
  private static final int MAXIMUM_RETRY_COUNTS = 5;

  private final int staleness;
  private final AggregationMaster aggregationMaster;
  private final ClockMsgCodec codec;

  /**
   * Clock table which contains current worker clocks.
   */
  private final Map<String, Integer> workerClockMap;

  /**
   * List of workers whose clocks are {@link ClockManager#globalMinimumClock}.
   */
  private final List<String> minimumClockWorkers;

  /**
   * The minimum clock among all workers.
   */
  private int globalMinimumClock;

  @Inject
  private ClockManager(final AggregationMaster aggregationMaster,
                       final ClockMsgCodec codec,
                       @Parameter(Staleness.class) final int staleness) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.staleness = staleness;
    this.globalMinimumClock = INITIAL_GLOBAL_MINIMUM_CLOCK;
    workerClockMap = new HashMap<>();
    minimumClockWorkers = new ArrayList<>();
  }

  /**
   * Helper function to create broadcast global minimum clock message.
   */
  public static AvroClockMsg getBroadcastMinClockMessage(final int globalMinimumClock) {
    final BroadcastMinClockMsg broadcastMinClockMsg =
        BroadcastMinClockMsg.newBuilder().setGlobalMinClock(globalMinimumClock).build();
    return AvroClockMsg.newBuilder()
        .setType(ClockMsgType.BroadcastMinClockMsg)
        .setBroadcastMinClockMsg(broadcastMinClockMsg).build();
  }

  /**
   * Helper function to create initial clock message.
   */
  public static AvroClockMsg getReplyInitialClockMessage(final int globalMinimumClock, final int workerClock) {
    final ReplyInitClockMsg replyInitClockMsg =
        ReplyInitClockMsg.newBuilder()
            .setGlobalMinClock(globalMinimumClock)
            .setInitClock(workerClock)
            .build();
    return AvroClockMsg.newBuilder()
        .setType(ClockMsgType.ReplyInitClockMsg)
        .setReplyInitClockMsg(replyInitClockMsg).build();
  }

  /**
   * Set initial clock of new worker which is not added by EM.
   * Worker added by EM should have globalMinimumClock + (staleness/2) as its worker clock
   * and it is set when the worker requests initialization(not creation time).
   * @param addedEval true means worker is added by EM, otherwise false
   * @param workerId  the id of new worker
   */
  public synchronized void onWorkerAdded(final boolean addedEval, final String workerId) {
    if (addedEval) {
      return;
    }
    // check whether all of initial workers which are not added by EM
    // are added before {@link ClockManager#globalMinimumClock} is changed
    if (!addedEval && globalMinimumClock != INITIAL_GLOBAL_MINIMUM_CLOCK) {
      throw new RuntimeException("initial workers are added after global minimum clock change");
    }
    workerClockMap.put(workerId, globalMinimumClock);
    minimumClockWorkers.add(workerId);
  }

  /**
   * Remove the entry according to the workerId from {@link ClockManager#workerClockMap}.
   * Update global minimum clock if the worker is the last one of {@link ClockManager#minimumClockWorkers}.
   * @param workerId the worker id to be deleted
   */
  public synchronized void onWorkerDeleted(final String workerId) {
    workerClockMap.remove(workerId);
    if (minimumClockWorkers.remove(workerId)) {
      if (minimumClockWorkers.size() == 0) {
        globalMinimumClock = workerClockMap.isEmpty() ?
            INITIAL_GLOBAL_MINIMUM_CLOCK : Collections.min(workerClockMap.values());
        broadcastGlobalMinimumClock();
      }
    }
  }

  /**
   * Used for testing.
   */
  int getGlobalMinimumClock() {
    return globalMinimumClock;
  }

  /**
   * Used for testing.
   */
  Integer getClockOf(final String workerId) {
    return workerClockMap.get(workerId);
  }

  /**
   * Initialize worker clock and put into {@link ClockManager#workerClockMap}.
   * @param workerId the worker id to initialize
   * @return the initial worker clock in {@link ClockManager#workerClockMap}
   */
  private synchronized int initializeWorkerClock(final String workerId) {
    final Integer workerClockVal = workerClockMap.get(workerId);
    // initial workers(which are not added by EM) have their clocks in {@link ClockManager#workerClockMap} already,
    // their clocks are set on onWorkerAdded() call.
    if (workerClockVal != null) {
      return workerClockVal;
    }
    final int workerClock = globalMinimumClock + (staleness / 2);
    workerClockMap.put(workerId, workerClock);
    return workerClock;
  }

  /**
   * Tick the clock of workerId and update {@link ClockManager#globalMinimumClock} if it is necessary.
   * When the worker according to the wokrerId is the last one of {@link ClockManager#minimumClockWorkers},
   * it's time to update {@link ClockManager#globalMinimumClock}.
   * @param workerId the worker id to tick clock
   */
  private synchronized void tickClock(final String workerId) {
    Integer workerClock = workerClockMap.get(workerId);
    // clock should be initialized and stored in the {@link ClockManager#workerClockMap} before tick
    if (workerClock == null) {
      throw new RuntimeException("uninitialized worker is ticked");
    }
    // tick the worker clock
    workerClock++;
    workerClockMap.put(workerId, workerClock);
    // remove the worker from {@link ClockManager#minimumClockWorkers}
    if (minimumClockWorkers.remove(workerId)) {
      if (minimumClockWorkers.size() == 0) {
        globalMinimumClock++;
        broadcastGlobalMinimumClock();
      }
    }
  }

  /**
   * Broadcast updated global minimum clock to all workers.
   * All of the workers whose clocks are same with {@link ClockManager#globalMinimumClock}
   * are added to {@link ClockManager#minimumClockWorkers}.
   */
  private void broadcastGlobalMinimumClock() {
    // {@link ClockManager#minimumClockWorkers} must be empty now and filled by this call
    if (!minimumClockWorkers.isEmpty()) {
      throw new RuntimeException("minimum worker clock list should be empty and updated now");
    }

    for (final Map.Entry<String, Integer> elem : workerClockMap.entrySet()) {
      final String workerId = elem.getKey();

      int tryCount = 0;
      while (tryCount++ < MAXIMUM_RETRY_COUNTS) {
        try {
          final byte[] data = codec.encode(getBroadcastMinClockMessage(globalMinimumClock));
          aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, data);
          break;
        } catch (final NetworkException e) {
          LOG.log(Level.INFO, "Target worker failed to receive global minimum clock message", e);
          if (tryCount == MAXIMUM_RETRY_COUNTS) {
            throw new RuntimeException(e);
          }
        }
      }

      if (elem.getValue() == globalMinimumClock) {
        minimumClockWorkers.add(workerId);
      }
    }
  }

  public final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final AvroClockMsg rcvMsg = codec.decode(aggregationMessage.getData().array());
      final String workerId = aggregationMessage.getSourceId().toString();
      switch (rcvMsg.getType()) {
      case RequestInitClockMsg:
        final int workerClock = initializeWorkerClock(workerId);
        final byte[] data = codec.encode(getReplyInitialClockMessage(globalMinimumClock, workerClock));

        int tryCount = 0;
        while (tryCount++ < MAXIMUM_RETRY_COUNTS) {
          try {
            aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, data);
            break;
          } catch (final NetworkException e) {
            LOG.log(Level.INFO, "Target worker failed to receive the initial worker clock message.", e);
            if (tryCount == MAXIMUM_RETRY_COUNTS) {
              throw new RuntimeException(e);
            }
          }
        }
        break;
      case TickMsg:
        tickClock(workerId);
        break;
      default:
        throw new RuntimeException("Unexpected message type: " + rcvMsg.getType().toString());
      }
    }
  }
}
