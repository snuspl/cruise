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
package edu.snu.cay.services.shuffle.evaluator.operator.impl;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.evaluator.operator.BaseShuffleSender;
import edu.snu.cay.services.shuffle.evaluator.operator.ShuffleTupleMessageGenerator;
import edu.snu.cay.services.shuffle.network.ShuffleTupleLinkListener;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessage;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of BaseShuffleSender.
 */
public final class BaseShuffleSenderImpl<K, V> implements BaseShuffleSender<K, V> {

  private final String shuffleName;
  private final ShuffleDescription shuffleDescription;
  private final ShuffleStrategy<K> shuffleStrategy;
  private final ShuffleTupleLinkListener shuffleTupleLinkListener;
  private final ConnectionFactory<ShuffleTupleMessage> tupleMessageConnectionFactory;
  private final IdentifierFactory idFactory;
  private final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator;

  @Inject
  private BaseShuffleSenderImpl(
      final ShuffleDescription shuffleDescription,
      final ShuffleStrategy<K> shuffleStrategy,
      final ShuffleTupleLinkListener shuffleTupleLinkListener,
      final NetworkConnectionService networkConnectionService,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final ShuffleTupleMessageGenerator<K, V> tupleMessageGenerator) {
    this.shuffleName = shuffleDescription.getShuffleName();
    this.shuffleDescription = shuffleDescription;
    this.shuffleStrategy = shuffleStrategy;
    this.shuffleTupleLinkListener = shuffleTupleLinkListener;
    this.tupleMessageConnectionFactory = networkConnectionService
        .getConnectionFactory(idFactory.getNewInstance(ShuffleParameters.NETWORK_CONNECTION_SERVICE_ID));
    this.idFactory = idFactory;
    this.tupleMessageGenerator = tupleMessageGenerator;
  }

  @Override
  public List<String> sendTuple(final Tuple<K, V> tuple) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createTupleMessageAndReceiverList(tuple));
  }

  @Override
  public List<String> sendTuple(final List<Tuple<K, V>> tupleList) {
    return sendShuffleMessageTupleList(tupleMessageGenerator.createTupleMessageAndReceiverList(tupleList));
  }

  @Override
  public void sendTupleTo(final String receiverId, final Tuple<K, V> tuple) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(receiverId, tupleMessageGenerator.createTupleMessage(tuple)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
  public void sendTupleTo(final String receiverId, final List<Tuple<K, V>> tupleList) {
    final List<Tuple<String, ShuffleTupleMessage<K, V>>> shuffleMessageTupleList = new ArrayList<>(1);
    shuffleMessageTupleList.add(new Tuple<>(receiverId, tupleMessageGenerator.createTupleMessage(tupleList)));
    sendShuffleMessageTupleList(shuffleMessageTupleList);
  }

  @Override
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

  @Override
  public ShuffleStrategy<K> getShuffleStrategy() {
    return shuffleStrategy;
  }

  @Override
  public List<String> getSelectedReceiverIdList(final K key) {
    return shuffleStrategy.selectReceivers(key, shuffleDescription.getReceiverIdList());
  }
}
