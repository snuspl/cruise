/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.shuffle.example.push;

import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.ShuffleProvider;
import edu.snu.cay.services.shuffle.evaluator.operator.PushDataListener;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receive tuples from SenderTasks.
 */
public final class ReceiverTask implements Task {

  private static final Logger LOG = Logger.getLogger(SenderTask.class.getName());
  private AtomicInteger totalNumReceivedTuples;
  private final AtomicInteger numCompletedIterations;
  private final int numTotalIterations;

  @Inject
  private ReceiverTask(final ShuffleProvider shuffleProvider,
      @Parameter(MessageExchangeParameters.TotalIterationNum.class) final int numTotalIterations) {
    this.numCompletedIterations = new AtomicInteger();
    final Shuffle<Integer, Integer> shuffle = shuffleProvider
        .getShuffle(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    final PushShuffleReceiver<Integer, Integer> shuffleReceiver = shuffle.getReceiver();
    shuffleReceiver.registerDataListener(new DataReceiver());
    this.totalNumReceivedTuples = new AtomicInteger();
    this.numTotalIterations = numTotalIterations;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    synchronized (this) {
      this.wait();
    }

    final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(totalNumReceivedTuples.get());
    return byteBuffer.array();
  }

  private final class DataReceiver implements PushDataListener<Integer, Integer> {

    @Override
    public void onTupleMessage(final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      for (final ShuffleTupleMessage<Integer, Integer> shuffleMessage : message.getData()) {
        final int numReceivedTuples = shuffleMessage.size();
        LOG.log(Level.INFO, "{0} tuples arrived from {1}", new Object[]{numReceivedTuples, message.getSrcId()});
        totalNumReceivedTuples.addAndGet(numReceivedTuples);
      }
    }

    @Override
    public void onComplete() {
      final int numIterations = numCompletedIterations.incrementAndGet();
      LOG.log(Level.INFO, "{0} th iteration completed", numIterations);
      if (numIterations == numTotalIterations) {
        LOG.log(Level.INFO, "The final iteration was completed");
        synchronized (ReceiverTask.this) {
          ReceiverTask.this.notify();
        }
      }
    }

    @Override
    public void onShutdown() {
      LOG.log(Level.INFO, "The receiver was shutdown by the manager");
      synchronized (ReceiverTask.this) {
        ReceiverTask.this.notify();
      }
    }
  }
}
