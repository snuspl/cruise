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
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleTupleMessageGenerator;
import edu.snu.cay.services.shuffle.network.ShuffleTupleLinkListener;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
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
import java.util.List;

/**
 * Data sender that has base operation to send tuples.
 *
 * Users can register link listener for ShuffleTupleMessage to track whether the message
 * sent from this operator has been transferred successfully.
 */
public final class DataSender<K, V> {

  private final String shuffleName;
  private final ShuffleTupleLinkListener shuffleTupleLinkListener;
  private final ConnectionFactory<ShuffleTupleMessage> tupleMessageConnectionFactory;
  private final IdentifierFactory idFactory;
  private final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator;

  @Inject
  private DataSender(
      final ShuffleDescription shuffleDescription,
      final ShuffleTupleLinkListener shuffleTupleLinkListener,
      final ShuffleNetworkSetup shuffleNetworkSetup,
      final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory) {
    this.shuffleName = shuffleDescription.getShuffleName();
    this.shuffleTupleLinkListener = shuffleTupleLinkListener;
    this.tupleMessageConnectionFactory = shuffleNetworkSetup.getTupleConnectionFactory();
    this.tupleMessageGenerator = tupleMessageGenerator;
    this.idFactory = idFactory;
  }

  /**
   * Send a tuple to selected receivers using ShuffleStrategy of the shuffle.
   *
   * @param tuple a tuple
   * @return the selected receiver id list
   */
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createTupleMessageAndReceiverList(tuple));
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
    return sendShuffleMessageTupleList(tupleMessageGenerator.createTupleMessageAndReceiverList(tupleList));
  }

  /**
   * Send a tuple to the specific receiver.
   *
   * @param receiverId a receiver id
   * @param tuple a tuple
   */
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(receiverId, tupleMessageGenerator.createTupleMessage(tuple)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  /**
   * Send a tuple list to the specific receiver. Note that this method does not use ShuffleStrategy to select
   * receivers and send all of tuples in tuple list to the same receiver.
   *
   * @param receiverId a receiver id
   * @param tupleList a tuple list
   */
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(receiverId, tupleMessageGenerator.createTupleMessage(tupleList)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  /**
   * Register a link listener to listen to whether the messages successfully sent through this sender.
   *
   * @param linkListener link listener for ShuffleTupleMessage
   */
  public void registerTupleLinkListener(final LinkListener<Message<ShuffleTupleMessage<K, V>>> linkListener) {
    shuffleTupleLinkListener.registerLinkListener(shuffleName, (LinkListener) linkListener);
  }

  private List<String> sendShuffleMessageTupleList(
      final List<Tuple<String, ShuffleTupleMessage<K, V>>> messageTupleList) {
    final List<String> receiverList = new ArrayList<>(messageTupleList.size());
    for (final Tuple<String, ShuffleTupleMessage<K, V>> shuffleMessageTuple : messageTupleList) {
      sendShuffleMessageTuple(shuffleMessageTuple);
      receiverList.add(shuffleMessageTuple.getKey());
    }

    return receiverList;
  }

  private void sendShuffleMessageTuple(final Tuple<String, ShuffleTupleMessage<K, V>> messageTuple) {
    try {
      final Connection<ShuffleTupleMessage> connection = tupleMessageConnectionFactory
          .newConnection(idFactory.getNewInstance(messageTuple.getKey()));
      connection.open();
      connection.write(messageTuple.getValue());
    } catch (final NetworkException exception) {
      throw new RuntimeException(exception);
    }
  }
}
