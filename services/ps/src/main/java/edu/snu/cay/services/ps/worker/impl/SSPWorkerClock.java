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
package edu.snu.cay.services.ps.worker.impl;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import edu.snu.cay.services.ps.avro.AvroClockMsg;
import edu.snu.cay.services.ps.avro.ClockMsgType;
import edu.snu.cay.services.ps.avro.RequestInitClockMsg;
import edu.snu.cay.services.ps.avro.TickMsg;
import edu.snu.cay.services.ps.driver.impl.ClockManager;
import edu.snu.cay.services.ps.ns.ClockMsgCodec;
import edu.snu.cay.services.ps.worker.api.WorkerClock;
import edu.snu.cay.services.ps.worker.parameters.Staleness;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;

/**
 * A worker clock of SSP model.
 *
 * Receive the global minimum clock from the driver and send the worker clock to the driver.
 * clock() is called once per each iteration.
 */
@EvaluatorSide
@Unit
public final class SSPWorkerClock implements WorkerClock {

  private final AggregationSlave aggregationSlave;

  private final ClockMsgCodec codec;

  /**
   * The latch to wait until ReplyInitClockMsg arrive.
   * The message is sent only once.
   */
  private final CountDownLatch initLatch;

  /**
   * The latch to wait until BroadcastMinClockMsg arrive.
   * The message is sent multiple times, so the latch should be reinitialized many times.
   */
  private CountDownLatch globalMinClockLatch;

  private final int staleness;

  private int workerClock;

  /**
   * The minimum clock among all worker clocks.
   */
  private int globalMinimumClock;

  @Inject
  private SSPWorkerClock(@Parameter(Staleness.class) final int staleness,
                         final AggregationSlave aggregationSlave,
                         final ClockMsgCodec codec) {
    this.staleness = staleness;
    this.aggregationSlave = aggregationSlave;
    this.codec = codec;
    this.initLatch = new CountDownLatch(1);
    this.globalMinClockLatch = new CountDownLatch(1);
    this.workerClock = -1;
    this.globalMinimumClock = -1;
  }

  @Override
  public void initialize() {
    final AvroClockMsg avroClockMsg =
        AvroClockMsg.newBuilder()
        .setType(ClockMsgType.RequestInitClockMsg)
        .setRequestInitClockMsg(RequestInitClockMsg.newBuilder().build())
        .build();
    final byte[] data = codec.encode(avroClockMsg);
    aggregationSlave.send(ClockManager.AGGREGATION_CLIENT_NAME, data);

    // wait until to get current global minimum clock and initial worker clock
    try {
      initLatch.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }

  @Override
  public void clock() {
    workerClock++;
    final AvroClockMsg avroClockMsg =
        AvroClockMsg.newBuilder()
            .setType(ClockMsgType.TickMsg)
            .setTickMsg(TickMsg.newBuilder().build())
            .build();
    final byte[] data = codec.encode(avroClockMsg);
    aggregationSlave.send(ClockManager.AGGREGATION_CLIENT_NAME, data);
  }

  @Override
  public void waitIfExceedingStalenessBound() throws InterruptedException {
    while (workerClock > globalMinimumClock + staleness) {
      globalMinClockLatch.await();
    }
  }

  @Override
  public int getWorkerClock() {
    return workerClock;
  }

  @Override
  public int getGlobalMinimumClock() {
    return globalMinimumClock;
  }

  public final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final AvroClockMsg avroClockMsg = codec.decode(aggregationMessage.getData().array());
      switch (avroClockMsg.getType()) {
      case ReplyInitClockMsg:
        globalMinimumClock = avroClockMsg.getReplyInitClockMsg().getGlobalMinClock();
        workerClock = avroClockMsg.getReplyInitClockMsg().getInitClock();
        initLatch.countDown();
        break;
      case BroadcastMinClockMsg:
        globalMinimumClock = avroClockMsg.getBroadcastMinClockMsg().getGlobalMinClock();
        globalMinClockLatch.countDown();
        globalMinClockLatch = new CountDownLatch(1);
        break;
      default:
        throw new RuntimeException("Unexpected message type: " + avroClockMsg.getType().toString());
      }
    }
  }
}
