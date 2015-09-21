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
package edu.snu.cay.services.shuffle.network;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;

/**
 * Setup a connection factory of Tuple.
 */
public final class TupleNetworkSetup<K, V> implements AutoCloseable {

  private static final String SHUFFLE_TUPLE_CONNECTION_FACTORY_ID_PREFIX = "SHUFFLE_TUPLE_";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier tupleConnectionFactoryId;
  private final ShuffleTupleMessageHandler<K, V> tupleMessageHandler;
  private final ShuffleTupleLinkListener<K, V> tupleLinkListener;
  private final ConnectionFactory<Tuple<K, V>> connectionFactory;

  @Inject
  private TupleNetworkSetup(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.EndPointId.class) final String endPointId,
      final NetworkConnectionService networkConnectionService,
      final ShuffleTupleMessageHandler<K, V> tupleMessageHandler,
      final ShuffleTupleLinkListener<K, V> tupleLinkListener,
      final TupleCodec<K, V> tupleMessageCodec) {
    this.networkConnectionService = networkConnectionService;
    this.tupleMessageHandler = tupleMessageHandler;
    this.tupleLinkListener = tupleLinkListener;
    this.tupleConnectionFactoryId = idFactory
        .getNewInstance(getTupleConnectionFactoryId(shuffleName));

    final Identifier endPointIdentifier = idFactory.getNewInstance(endPointId);
    this.connectionFactory = networkConnectionService.registerConnectionFactory(
        tupleConnectionFactoryId,
        tupleMessageCodec,
        tupleMessageHandler,
        tupleLinkListener,
        endPointIdentifier
    );
  }

  private String getTupleConnectionFactoryId(final String shuffleName) {
    return SHUFFLE_TUPLE_CONNECTION_FACTORY_ID_PREFIX + shuffleName;
  }

  /**
   * Set a message handler for Tuple.
   *
   * @param messageHandler a message handler of Tuple
   */
  public void setTupleMessageHandler(final EventHandler<Message<Tuple<K, V>>> messageHandler) {
    tupleMessageHandler.setTupleMessageHandler(messageHandler);
  }

  /**
   * Set a message handler and a link listener for Tuple.
   *
   * @param linkListener a link listener of Tuple
   */
  public void setTupleLinkListener(final LinkListener<Message<Tuple<K, V>>> linkListener) {
    tupleLinkListener.setTupleLinkListener(linkListener);
  }

  /**
   * @return the connection factory for Tuple
   */
  public ConnectionFactory<Tuple<K, V>> getTupleConnectionFactory() {
    return connectionFactory;
  }

  @Override
  public void close() {
    networkConnectionService.unregisterConnectionFactory(tupleConnectionFactoryId);
  }
}
