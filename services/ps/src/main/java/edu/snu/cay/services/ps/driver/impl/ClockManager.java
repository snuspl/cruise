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
import edu.snu.cay.services.ps.worker.parameters.Staleness;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.SerializableCodec;
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
 * Receive all worker clocks and save the updated worker clock to the {@link }
 * Broadcast the global minimum clock among all workers when it is changed.
 */
@DriverSide
@Unit
public final class ClockManager {
  private static final Logger LOG = Logger.getLogger(ClockManager.class.getName());

  public static final String AGGREGATION_CLIENT_NAME = ClockManager.class.getName();
  /**
   * protocol format : BROADCAST_GLOBAL_MINIMUM_CLOCK/<global_minimum_clock(integer)>.
   */
  public static final String BROADCAST_GLOBAL_MINIMUM_CLOCK = "broadcastGlobalMinimumClock";
  public static final String REQUEST_INITIAL_CLOCK = "requestInitialClock";
  /**
   * protocol format :
   * SET_INITIAL_CLOCK/<global minimum clock(integer)>/<initial worker clock(integer)>.
   */
  public static final String SET_INITIAL_CLOCK = "setInitialClock";
  public static final String TICK = "tick";

  private final AggregationMaster aggregationMaster;
  private final SerializableCodec<String> codec;
  private final int staleness;
  private int globalMinimumClock;
  private final Map<String, Integer> workerClockMap;
  /**
   * List of workers whose clocks are globalMinimumClock.
   */
  private final List<String> minimumClockWorkers;

  @Inject
  private ClockManager(final AggregationMaster aggregationMaster,
                       final SerializableCodec<String> codec,
                       @Parameter(Staleness.class) final int staleness) {
    this.aggregationMaster = aggregationMaster;
    this.codec = codec;
    this.staleness = staleness;
    this.globalMinimumClock = 0;
    workerClockMap = new HashMap<String, Integer>();
    minimumClockWorkers = new ArrayList<String>();
  }

  /**
   * Set initial clock of new worker which is not added by EM.
   * Worker added by EM should have globalMinimumClock + (staleness/2) as its worker clock
   * and it is set when worker clock is initialized(not created).
   *
   * @param addedEval true means worker is added by EM, otherwise false
   * @param workerId  the id of new worker
   */
  public synchronized void onWorkerAdded(final boolean addedEval, final String workerId) {
    if (addedEval) {
      return;
    }
    workerClockMap.put(workerId, globalMinimumClock);
    minimumClockWorkers.add(workerId);
  }

  /**
   * Remove the entry according to the workerId from workerClockMap.
   * Update global minimum clock if the worker is the last one of minimumClockWorkers.
   *
   * @param workerId the worker id to be deleted
   */
  public synchronized void onWorkerDeleted(final String workerId) {
    workerClockMap.remove(workerId);
    if (minimumClockWorkers.contains(workerId)) {
      minimumClockWorkers.remove(workerId);
      // If the worker is only one worker who has globalMinimumClock,
      // it's the time to update globalMinimumClock.
      if (minimumClockWorkers.size() == 0) {
        globalMinimumClock = workerClockMap.isEmpty() ? 0 : Collections.min(workerClockMap.values());
        broadcastGlobalMinimumClock();
      }
    }
  }

  /**
   * Used for testing.
   */
  public int getGlobalMinimumClock() {
    return globalMinimumClock;
  }

  /**
   * Used for testing.
   */
  public Integer getClockOf(final String workerId) {
    return workerClockMap.get(workerId);
  }

  /**
   * Initialize worker clock and put into workerClockMap.
   *
   * @param workerId the worker id to initialize
   * @return the initial worker clock in workerClockMap
   */
  private synchronized int setInitialWorkerClock(final String workerId) {
    final Integer workerClockVal = workerClockMap.get(workerId);
    // Initial workers(which are not added by EM) have their clocks in workerClockMap already,
    // its clocks are set on onWorkerAdded() call.
    if (workerClockVal != null) {
      return workerClockVal.intValue();
    }
    final int workerClock = globalMinimumClock + (staleness / 2);
    workerClockMap.put(workerId, workerClock);
    return workerClock;
  }

  /**
   * Tick the clock of workerId and update globalMinimumClock if it is necessary.
   *
   * @param workerId the worker id to tick clock
   */
  private synchronized void tickClock(final String workerId) {
    // clock should be initialized in the workerClockMap before tick
    assert (workerClockMap.containsKey(workerId));
    int workerClock = workerClockMap.get(workerId).intValue();
    // tick the worker clock
    workerClock++;
    workerClockMap.put(workerId, workerClock);
    if (minimumClockWorkers.contains(workerId)) {
      // remove the worker from minimum clock worker list
      minimumClockWorkers.remove(workerId);

      // If the worker is only one worker who has globalMinimumClock,
      // it's the time to update globalMinimumClock.
      if (minimumClockWorkers.size() == 0) {
        globalMinimumClock++;
        broadcastGlobalMinimumClock();
      }
    } else if (workerClock == globalMinimumClock) {
      minimumClockWorkers.add(workerId);
    }
  }

  /**
   * Broadcast updated global minimum clock to all workers.
   * All of the workers whose clocks are same with globalMinimumClock are added
   * to minimumClockWorkers.
   */
  private void broadcastGlobalMinimumClock() {
    // minimumClockWorkers must be empty now and filled by this call
    assert (minimumClockWorkers.isEmpty());

    for (final Map.Entry<String, Integer> elem : workerClockMap.entrySet()) {
      final String workerId = elem.getKey();
      try {
        final byte[] data = codec.encode(getBroadcastGlobalMinimumClockMessage());
        aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, data);
      } catch (final NetworkException e) {
        LOG.log(Level.INFO, "Target worker failed to receive global minimum clock message", e);
      }

      if (elem.getValue() == globalMinimumClock) {
        minimumClockWorkers.add(workerId);
      }
    }
  }

  private String getBroadcastGlobalMinimumClockMessage() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append(BROADCAST_GLOBAL_MINIMUM_CLOCK);
    buffer.append("/");
    buffer.append(globalMinimumClock);
    return buffer.toString();
  }

  private String getInitialClockMessage(final int workerClock) {
    final StringBuffer buffer = new StringBuffer();
    buffer.append(SET_INITIAL_CLOCK)
        .append("/")
        .append(globalMinimumClock)
        .append("/")
        .append(workerClock);
    return buffer.toString();
  }

  public final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final String rcvMsg = codec.decode(aggregationMessage.getData().array());
      final String workerId = aggregationMessage.getSourceId().toString();
      switch (rcvMsg) {
      case REQUEST_INITIAL_CLOCK:
        final int workerClock = setInitialWorkerClock(workerId);
        final byte[] data = codec.encode(getInitialClockMessage(workerClock));
        try {
          aggregationMaster.send(AGGREGATION_CLIENT_NAME, workerId, data);
        } catch (final NetworkException e) {
          LOG.log(Level.INFO, "Target worker failed to receive the initial worker clock message.", e);
        }
        break;
      case TICK:
        tickClock(workerId);
        break;
      default:
      }
    }
  }
}
