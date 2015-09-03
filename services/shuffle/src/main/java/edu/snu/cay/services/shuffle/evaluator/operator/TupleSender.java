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
package edu.snu.cay.services.shuffle.evaluator.operator;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.ESNetworkSetup;
import edu.snu.cay.services.shuffle.network.ShuffleTupleLinkListener;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sender that has base operation to send tuples.
 *
 * ESNetworkSetup should be injected first to use this class.
 *
 * Users can register link listener for ShuffleTupleMessage to track whether the message
 * sent from this operator has been transferred successfully.
 *
 * Note that Shuffle, ShuffleSender, ShuffleReceiver can not send tuples
 * through this class in the constructor of them.
 */
public final class TupleSender<K, V> {

  private final ShuffleDescription shuffleDescription;
  private final ShuffleTupleLinkListener<K, V> shuffleTupleLinkListener;
  private final ConnectionFactory<Tuple<K, V>> tupleMessageConnectionFactory;
  private final ShuffleStrategy<K> shuffleStrategy;
  private final IdentifierFactory idFactory;

  @Inject
  private TupleSender(
      final ShuffleDescription shuffleDescription,
      final ShuffleTupleLinkListener<K, V> shuffleTupleLinkListener,
      final ESNetworkSetup networkSetup,
      final ShuffleStrategy<K> shuffleStrategy,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory) {
    this.shuffleDescription = shuffleDescription;
    this.shuffleTupleLinkListener = shuffleTupleLinkListener;
    this.tupleMessageConnectionFactory = networkSetup.getTupleConnectionFactory();
    this.shuffleStrategy = shuffleStrategy;
    this.idFactory = idFactory;
  }

  /**
   * Send a tuple to selected receivers using ShuffleStrategy of the shuffle.
   *
   * @param tuple a tuple
   * @return the selected receiver id list
   */
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    final List<String> selectedReceiverIdList = shuffleStrategy
        .selectReceivers(tuple.getKey(), shuffleDescription.getReceiverIdList());
    for (final String receiverId : selectedReceiverIdList) {
      sendTupleTo(receiverId, tuple);
    }
    return selectedReceiverIdList;
  }

  /**
   * Send a tupleList to selected receivers using ShuffleStrategy of the shuffle.
   *
   * Each tuple in the tupleList can be sent to many receivers, so the tuples to the same end point are
   * chunked into one ShuffleTupleMessage.
   *
   * @param tupleList a tuple list
   * @return the selected receiver id list
   */
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    final Map<String, List<Tuple<K, V>>> tupleListMap = new HashMap<>();
    final List<String> receiverIdList = shuffleDescription.getReceiverIdList();
    for (final Tuple<K, V> tuple : tupleList) {
      for (final String receiverId : shuffleStrategy.selectReceivers(tuple.getKey(), receiverIdList)) {
        if (!tupleListMap.containsKey(receiverId)) {
          tupleListMap.put(receiverId, new ArrayList<Tuple<K, V>>());
        }

        tupleListMap.get(receiverId).add(tuple);
      }
    }

    final List<String> selectedReceiverIdList = new ArrayList<>(tupleListMap.keySet());
    for (final String receiverId : selectedReceiverIdList) {
      sendTupleTo(receiverId, tupleListMap.get(receiverId));
    }

    return selectedReceiverIdList;
  }

  /**
   * Send a tuple to the specific receiver. Note that this method does not use ShuffleStrategy to select
   * receivers and send the tuple to the receiver.
   *
   * @param receiverId a receiver id
   * @param tuple a tuple
   */
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    try {
      final Connection<Tuple<K, V>> connection = tupleMessageConnectionFactory
          .newConnection(idFactory.getNewInstance(receiverId));
      connection.open();
      connection.write(tuple);
    } catch (final NetworkException exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Send a tuple list to the specific receiver. Note that this method does not use ShuffleStrategy to select
   * receivers and send all of tuples in tuple list to the same receiver.
   *
   * @param receiverId a receiver id
   * @param tupleList a tuple list
   */
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    try {
      final Connection<Tuple<K, V>> connection = tupleMessageConnectionFactory
          .newConnection(idFactory.getNewInstance(receiverId));
      connection.open();
      connection.write(tupleList);
    } catch (final NetworkException exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Register a link listener to listen to whether the messages successfully sent through this sender.
   *
   * @param linkListener link listener for ShuffleTupleMessage
   */
  public void setTupleLinkListener(final LinkListener<Message<Tuple<K, V>>> linkListener) {
    shuffleTupleLinkListener.setTupleLinkListener(linkListener);
  }
}
