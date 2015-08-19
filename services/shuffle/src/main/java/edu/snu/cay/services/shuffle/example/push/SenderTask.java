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
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import org.apache.reef.io.Tuple;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Send tuples to ReceiverTasks and SenderReceiverTasks.
 */
public final class SenderTask implements Task {

  private static final Logger LOG = Logger.getLogger(SenderTask.class.getName());

  private final PushShuffleSender<Integer, Integer> shuffleSender;
  private final int receiverNum;

  @Inject
  private SenderTask(final ShuffleProvider shuffleProvider) {
    final Shuffle<Integer, Integer> shuffle = shuffleProvider
        .getShuffle(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    this.shuffleSender = shuffle.getSender();
    this.receiverNum = shuffle.getShuffleDescription().getReceiverIdList().size();
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    int sentTupleCount = 0;
    for (int i = 0; i < MessageExchangeDriver.ITERATION_NUMBER; i++) {
      for (int j = 0; j < MessageExchangeDriver.NETWORK_MESSAGE_NUMBER_IN_ONE_ITERATION; j++) {
        LOG.log(Level.INFO, "Send tuple messages");
        final List<Tuple<Integer, Integer>> tupleList = generateRandomTuples();
        sentTupleCount += tupleList.size();
        shuffleSender.sendTuple(tupleList);
      }

      LOG.log(Level.INFO, "Complete to send tuples and wait for all receivers received tuples from all senders");
      shuffleSender.completeAndWaitForReceivers();
      LOG.log(Level.INFO, "Finish iteration " + i);
    }
    final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(sentTupleCount);
    return byteBuffer.array();
  }


  private List<Tuple<Integer, Integer>> generateRandomTuples() {
    final Random rand = new Random();
    final List<Tuple<Integer, Integer>> randomTupleList = new ArrayList<>();
    final int tupleListSize = receiverNum + rand.nextInt(receiverNum);
    for (int i = 0; i < tupleListSize; i++) {
      randomTupleList.add(new Tuple<>(rand.nextInt(receiverNum * 3), rand.nextInt()));
    }

    return randomTupleList;
  }
}
