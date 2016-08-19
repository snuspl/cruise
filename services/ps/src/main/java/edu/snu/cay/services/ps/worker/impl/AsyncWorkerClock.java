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
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A worker clock for non-SSP model.
 *
 * Receive the global minimum clock from the driver and send the worker clock to the driver.
 * clock() is called once per each iteration.
 *
 * Differently from {@link SSPWorkerClock},
 * it only cares about initializing the local clock with the global minimum clock.
 */
@EvaluatorSide
@Unit
public final class AsyncWorkerClock implements WorkerClock {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerClock.class.getName());

  private final AggregationSlave aggregationSlave;

  private final ClockMsgCodec codec;

  /**
   * The latch to wait until ReplyInitClockMsg arrive.
   * The message is sent only once.
   */
  private final CountDownLatch initLatch = new CountDownLatch(1);

  private int workerClock;

  @Inject
  private AsyncWorkerClock(final AggregationSlave aggregationSlave,
                           final ClockMsgCodec codec) {
    this.codec = codec;
    this.aggregationSlave = aggregationSlave;
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
  public synchronized void clock() {
    workerClock++;
    LOG.log(Level.FINE, "Worker clock: {0}", workerClock);
    final AvroClockMsg avroClockMsg =
        AvroClockMsg.newBuilder()
            .setType(ClockMsgType.TickMsg)
            .setTickMsg(TickMsg.newBuilder().build())
            .build();
    final byte[] data = codec.encode(avroClockMsg);
    aggregationSlave.send(ClockManager.AGGREGATION_CLIENT_NAME, data);
  }

  @Override
  public void waitIfExceedingStalenessBound() {

  }

  @Override
  public void recordClockNetworkWaitingTime() {

  }

  @Override
  public int getWorkerClock() {
    return workerClock;
  }

  @Override
  public int getGlobalMinimumClock() {
    return 0;
  }

  public final class MessageHandler implements EventHandler<AggregationMessage> {

    @Override
    public void onNext(final AggregationMessage aggregationMessage) {
      final AvroClockMsg avroClockMsg = codec.decode(aggregationMessage.getData().array());
      switch (avroClockMsg.getType()) {
      case ReplyInitClockMsg:
        workerClock = avroClockMsg.getReplyInitClockMsg().getInitClock();
        LOG.log(Level.INFO, "Worker clock is initialized to {0}", workerClock);
        initLatch.countDown();
        break;
      case BroadcastMinClockMsg:
        // do not care
        break;
      default:
        throw new RuntimeException("Unexpected message type: " + avroClockMsg.getType().toString());
      }
    }
  }
}
