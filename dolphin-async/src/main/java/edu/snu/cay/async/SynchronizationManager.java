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
package edu.snu.cay.async;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A driver-side component that coordinates synchronization messages between the driver and workers.
 */
@DriverSide
@Unit
final class SynchronizationManager {

  private static final Logger LOG = Logger.getLogger(SynchronizationManager.class.getName());

  static final String AGGREGATION_CLIENT_NAME = SynchronizationManager.class.getName();

  private final AggregationMaster aggregationMaster;

  // TODO #379: This has to be correctly changed when the number of workers elastically scale-in or scale-out.
  private final int numWorkers;
  private final Set<String> blockedWorkerIds;

  @Inject
  private SynchronizationManager(final AggregationMaster aggregationMaster,
                                 final DataLoadingService dataLoadingService) {
    this.aggregationMaster = aggregationMaster;
    this.numWorkers = dataLoadingService.getNumberOfPartitions();
    this.blockedWorkerIds = Collections.synchronizedSet(new HashSet<String>());

    LOG.log(Level.FINE, "Total number of workers participating in the synchronization = {0}", numWorkers);
  }

  private void broadcastResponseMessages() {
    final Set<String> previousBlockedWorkerIds = new HashSet<>(blockedWorkerIds);

    blockedWorkerIds.clear();
    for (final String slaveId : previousBlockedWorkerIds) {
      aggregationMaster.send(AGGREGATION_CLIENT_NAME, slaveId, new byte[0]);
    }
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final String slaveId = aggregationMessage.getSourceId().toString();
      LOG.log(Level.FINE, "Receive a synchronization message from {0}. {1} messages have been received out of {2}.",
          new Object[]{slaveId, blockedWorkerIds.size(), numWorkers});

      if (blockedWorkerIds.contains(slaveId)) {
        LOG.log(Level.WARNING, "Multiple synchronization requests from {0}", slaveId);
      } else {
        blockedWorkerIds.add(slaveId);
      }

      if (blockedWorkerIds.size() == numWorkers) {
        LOG.log(Level.INFO, "{0} workers are blocked. Sending response messages to awake them", numWorkers);
        broadcastResponseMessages();
      }
    }
  }
}
