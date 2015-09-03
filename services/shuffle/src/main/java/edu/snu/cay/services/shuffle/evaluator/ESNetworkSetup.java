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

import edu.snu.cay.services.shuffle.network.*;
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
 * Evaluator-side network setup.
 *
 * Users should inject this class to register a connection factory of Tuple.
 * If they also want to create a connection factory of ShuffleControlMessage, they should
 * set a control message handler and a control link listener.
 */
public final class ESNetworkSetup implements AutoCloseable {

  private final NetworkConnectionService networkConnectionService;
  private final Identifier tupleConnectionFactoryId;
  private final Identifier controlConnectionFactoryId;
  private final Identifier endPointIdentifier;
  private final ShuffleControlMessageCodec controlMessageCodec;

  @Inject
  private ESNetworkSetup(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.EndPointId.class) final String endPointId,
      final NetworkConnectionService networkConnectionService,
      final TupleCodec tupleCodec,
      final ShuffleTupleMessageHandler tupleMessageHandler,
      final ShuffleTupleLinkListener tupleLinkListener,
      final ShuffleControlMessageCodec controlMessageCodec) {

    this.networkConnectionService = networkConnectionService;
    this.endPointIdentifier = idFactory.getNewInstance(endPointId);
    this.tupleConnectionFactoryId = idFactory
        .getNewInstance(ShuffleParameters.getTupleConnectionFactoryId(shuffleName));
    this.controlConnectionFactoryId = idFactory
        .getNewInstance(ShuffleParameters.getControlConnectionFactoryId(shuffleName));
    this.controlMessageCodec = controlMessageCodec;

    networkConnectionService.registerConnectionFactory(
        tupleConnectionFactoryId,
        tupleCodec,
        tupleMessageHandler,
        tupleLinkListener,
        endPointIdentifier
    );
  }

  /**
   * Register a connection factory of ShuffleControlMessage with controlMessageHandler and controlLinkListener.
   *
   * @param controlMessageHandler a message handler of ShuffleControlMessage
   * @param controlLinkListener a link listener of ShuffleControlMessage
   */
  public void setControlMessageHandlerAndLinkListener(
      final EventHandler<Message<ShuffleControlMessage>> controlMessageHandler,
      final LinkListener<Message<ShuffleControlMessage>> controlLinkListener) {
    networkConnectionService.registerConnectionFactory(
        controlConnectionFactoryId,
        controlMessageCodec,
        controlMessageHandler,
        controlLinkListener,
        endPointIdentifier
    );
  }

  /**
   * @return the connection factory for ShuffleControlMessage
   */
  public ConnectionFactory<ShuffleControlMessage> getControlConnectionFactory() {
    return networkConnectionService.getConnectionFactory(controlConnectionFactoryId);
  }

  /**
   * @return the connection factory for ShuffleTupleMessage
   */
  public <K, V> ConnectionFactory<Tuple<K, V>> getTupleConnectionFactory() {
    return networkConnectionService.getConnectionFactory(tupleConnectionFactoryId);
  }

  /**
   * Unregister connection factories.
   */
  @Override
  public void close() {
    networkConnectionService.unregisterConnectionFactory(tupleConnectionFactoryId);
    networkConnectionService.unregisterConnectionFactory(controlConnectionFactoryId);
  }
}
