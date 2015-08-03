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

import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.ShuffleProvider;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleReceiver;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleSender;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task for simple message exchanging example.
 */
public final class MessageExchangeTask implements Task {

  private static final Logger LOG = Logger.getLogger(MessageExchangeTask.class.getName());

  private final ShuffleSender<Integer, Integer> shuffleSender;
  private final List<String> receiverList;
  private final int taskNumber;
  private final Set<Identifier> receivedIdSet;
  private final CountDownLatch countDownLatch;

  @Inject
  private MessageExchangeTask(
      final ShuffleProvider shuffleProvider) {
    final Shuffle shuffle = shuffleProvider
        .getShuffle(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);

    this.shuffleSender = shuffle.getSender();
    shuffleSender.registerTupleLinkListener(new TupleLinkListener());

    final ShuffleReceiver<Integer, Integer> shuffleReceiver = shuffle
        .getReceiver();
    shuffleReceiver.registerTupleMessageHandler(new TupleMessageHandler());

    this.receiverList = shuffle.getShuffleDescription().getReceiverIdList();
    this.taskNumber = receiverList.size();
    this.countDownLatch = new CountDownLatch(taskNumber);
    this.receivedIdSet = new HashSet<>();
    Collections.synchronizedSet(this.receivedIdSet);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    // TODO: Currently MessageExchangeTasks sleep 3 seconds to wait the other tasks start.
    // Synchronization logic for all tasks in same shuffle will be included via another pull request.
    Thread.sleep(3000);
    final List<String> messageSentIdList = shuffleSender.sendTuple(generateRandomTuples());
    for (final String receiver : receiverList) {
      if (!messageSentIdList.contains(receiver)) {
        shuffleSender.sendTupleTo(receiver, new ArrayList<Tuple<Integer, Integer>>());
      }
    }

    countDownLatch.await();
    LOG.log(Level.INFO, "{0} messages are arrived. The task will be closed.", taskNumber);
    return null;
  }

  private List<Tuple<Integer, Integer>> generateRandomTuples() {
    final Random rand = new Random();
    final List<Tuple<Integer, Integer>> randomTupleList = new ArrayList<>();
    for (int i = 0; i < getTupleNumber(); i++) {
      randomTupleList.add(new Tuple<>(rand.nextInt(), rand.nextInt()));
    }
    return randomTupleList;
  }

  /**
   * The number of tuples is set to be less than the actual number of tasks in order to test the case where
   * the current task sends an empty message to some tasks.
   */
  private int getTupleNumber() {
    return taskNumber * 3 / 5;
  }

  private final class TupleLinkListener implements LinkListener<Message<ShuffleTupleMessage<Integer, Integer>>> {

    @Override
    public void onSuccess(final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      LOG.log(Level.FINE, "{0} was successfully sent.", message);
    }

    @Override
    public void onException(
        final Throwable cause,
        final SocketAddress remoteAddress,
        final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      throw new RuntimeException(cause);
    }
  }

  private final class TupleMessageHandler implements EventHandler<Message<ShuffleTupleMessage<Integer, Integer>>> {

    /**
     * It throws a RuntimeException if more than one message arrives from the same task
     * since only one message from one task is allowed.
     * The waiting task thread will be notified when all expected messages arrives.
     *
     * @param message a message from other nodes
     */
    @Override
    public void onNext(final Message<ShuffleTupleMessage<Integer, Integer>> message) {
      for (final ShuffleTupleMessage<Integer, Integer> tupleMessage : message.getData()) {
        if (tupleMessage.size() == 0) {
          LOG.log(Level.INFO, "An empty shuffle message arrived from {0}.", message.getSrcId());
        } else {
          LOG.log(Level.INFO, "A shuffle message with size {0} is arrived from {1}.",
              new Object[]{tupleMessage.size(), message.getSrcId()});
        }

        for (int i = 0; i < tupleMessage.size(); i++) {
          LOG.log(Level.INFO, tupleMessage.get(i).toString());
        }
      }

      if (receivedIdSet.contains(message.getSrcId())) {
        throw new RuntimeException("Only one message from one task is allowed.");
      } else {
        receivedIdSet.add(message.getSrcId());
        countDownLatch.countDown();
      }
    }
  }
}
