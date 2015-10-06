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
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SenderAndReceiverTask implements Task {

  private static final Logger LOG = Logger.getLogger(SenderAndReceiverTask.class.getName());

  private final ShuffleProvider shuffleProvider;
  private final PushShuffleSender<Integer, Integer> shuffleSender;
  private final int numTotalIterations;
  private final int totalNumReceivers;
  private final AtomicInteger totalNumReceivedTuples;
  private final AtomicInteger numCompletedIterations;
  private boolean isFinished;

  @Inject
  private SenderAndReceiverTask(
      final ShuffleProvider shuffleProvider,
      @Parameter(MessageExchangeParameters.TotalIterationNum.class) final int numTotalIterations) {
    this.shuffleProvider = shuffleProvider;
    final Shuffle<Integer, Integer> shuffle = shuffleProvider.
        getShuffle(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    this.shuffleSender = shuffle.getSender();
    this.totalNumReceivers = shuffle.getShuffleDescription().getReceiverIdList().size();
    final PushShuffleReceiver<Integer, Integer> shuffleReceiver = shuffle.getReceiver();
    shuffleReceiver.registerDataListener(new DataReceiver());

    this.numTotalIterations = numTotalIterations;
    this.totalNumReceivedTuples = new AtomicInteger();
    this.numCompletedIterations = new AtomicInteger();
  }

  @Override
  public byte[] call(byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "A SenderAndReceiverTask is started");
    int numSentTuples = 0;
    for (int i = 0; i < numTotalIterations; i++) {
      for (int j = 0; j < MessageExchangeDriver.NETWORK_MESSAGE_NUMBER_IN_ONE_ITERATION; j++) {
        LOG.log(Level.INFO, "Send tuple messages");
        final List<Tuple<Integer, Integer>> tupleList = generateRandomTuples();
        numSentTuples += tupleList.size();
        shuffleSender.sendTuple(tupleList);
      }

      LOG.log(Level.INFO, "Complete iteration {0}", (i + 1));
      final boolean isSenderShutdown = shuffleSender.complete();
      if (isSenderShutdown) {
        LOG.log(Level.INFO, "The sender was shutdown by the manager.");
        break;
      }
    }

    synchronized (this) {
      if (!isFinished) {
        this.wait();
      }
    }

    shuffleProvider.close();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putInt(numSentTuples);
    byteBuffer.putInt(totalNumReceivedTuples.get());
    return byteBuffer.array();
  }

  private void notifyReceiverFinished() {
    synchronized (this) {
      isFinished = true;
      this.notify();
    }
  }

  private List<Tuple<Integer, Integer>> generateRandomTuples() {
    final Random rand = new Random();
    final List<Tuple<Integer, Integer>> randomTupleList = new ArrayList<>();
    final int tupleListSize = totalNumReceivers * 10000 + rand.nextInt(totalNumReceivers) * 100;
    for (int i = 0; i < tupleListSize; i++) {
      randomTupleList.add(new Tuple<>(rand.nextInt(totalNumReceivers * 3), rand.nextInt()));
    }

    return randomTupleList;
  }

  private final class DataReceiver implements PushDataListener<Integer, Integer> {

    @Override
    public void onTupleMessage(final Message<Tuple<Integer, Integer>> message) {
      for (final Tuple<Integer, Integer> tuple : message.getData()) {
        final int numArrivedTuples = totalNumReceivedTuples.incrementAndGet();
        if (numArrivedTuples % 10000 == 0) {
          LOG.log(Level.INFO, "{0} tuples arrived", numArrivedTuples);
        }
      }
    }

    @Override
    public void onComplete() {
      final int numIterations = numCompletedIterations.incrementAndGet();
      LOG.log(Level.INFO, "{0} th iteration completed", numIterations);
      if (numIterations == numTotalIterations) {
        LOG.log(Level.INFO, "The final iteration was completed");
        notifyReceiverFinished();
      }
    }

    @Override
    public void onShutdown() {
      LOG.log(Level.INFO, "The receiver was shutdown by the manager");
      notifyReceiverFinished();
    }
  }
}
