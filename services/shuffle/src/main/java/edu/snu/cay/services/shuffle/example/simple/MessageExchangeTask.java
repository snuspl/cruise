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
package edu.snu.cay.services.shuffle.example.simple;

import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleSender;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.ShuffleProvider;
import org.apache.reef.io.Tuple;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * Task for simple message exchanging example.
 */
public final class MessageExchangeTask implements Task {

  private static final Logger LOG = Logger.getLogger(MessageExchangeTask.class.getName());

  private final PushShuffleSender<Integer, Integer> shuffleSender;
  private final PushShuffleReceiver<Integer, Integer> shuffleReceiver;
  private final int taskNumber;

  @Inject
  private MessageExchangeTask(
      final ShuffleProvider shuffleProvider) {
    final Shuffle<Integer, Integer> shuffle = shuffleProvider.getShuffle(
        MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    this.shuffleSender = shuffle.getSender();
    shuffleReceiver = shuffle.getReceiver();

    this.taskNumber = shuffle.getShuffleDescription().getReceiverIdList().size();
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    // TODO #88: Implement message exchanging using push-based shuffle.
    return null;
  }

  private List<Tuple<Integer, Integer>> generateRandomTuples() {
    final Random rand = new Random();
    final List<Tuple<Integer, Integer>> randomTupleList = new ArrayList<>();
    for (int i = 0; i < taskNumber * 2; i++) {
      randomTupleList.add(new Tuple<>(rand.nextInt(), rand.nextInt()));
    }
    return randomTupleList;
  }
}
