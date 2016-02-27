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
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronizes all workers by exchanging synchronization messages with the driver.
 */
@EvaluatorSide
@Unit
public final class WorkerSynchronizer {

  private static final Logger LOG = Logger.getLogger(WorkerSynchronizer.class.getName());

  private final AggregationSlave aggregationSlave;
  private final ResettingCountDownLatch countDownLatch;

  @Inject
  private WorkerSynchronizer(final AggregationSlave aggregationSlave) {
    this.aggregationSlave = aggregationSlave;
    this.countDownLatch = new ResettingCountDownLatch(1);
  }

  /**
   * Each worker sends a synchronization message to the driver and is blocked until
   * a response message arrives from the driver.
   */
  public void globalBarrier() {
    LOG.log(Level.INFO, "Sending a synchronization message to the driver");
    aggregationSlave.send(SynchronizationManager.AGGREGATION_CLIENT_NAME, new byte[0]);
    countDownLatch.awaitAndReset(1);
  }

  final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      LOG.log(Level.INFO, "Received a response message from the driver");
      countDownLatch.countDown();
    }
  }
}
