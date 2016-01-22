/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.core.metric.ns;

import edu.snu.cay.dolphin.core.metric.MetricsMessageCodec;
import edu.snu.cay.dolphin.core.metric.avro.MetricsMessage;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Register and unregister driver and evaluators to/from Network Connection Service.
 */
public final class MetricNetworkSetup {
  private static final String METRIC_COLLECTION_SERVICE_IDENTIFIER = "MCS";

  private final NetworkConnectionService networkConnectionService;
  private final Identifier connectionFactoryIdentifier;
  private final MetricsMessageCodec metricsMessageCodec;
  private final EventHandler<Message<MetricsMessage>> handler;
  private ConnectionFactory<MetricsMessage> connectionFactory;

  @Inject
  private MetricNetworkSetup(final NetworkConnectionService networkConnectionService,
                             final IdentifierFactory identifierFactory,
                             final MetricsMessageCodec metricsMessageCodec,
                             @Parameter(MetricsMessageHandler.class)
                             final EventHandler<Message<MetricsMessage>> handler) {
    this.networkConnectionService = networkConnectionService;
    this.connectionFactoryIdentifier = identifierFactory.getNewInstance(METRIC_COLLECTION_SERVICE_IDENTIFIER);
    this.metricsMessageCodec = metricsMessageCodec;
    this.handler = handler;
  }

  public ConnectionFactory<MetricsMessage> registerConnectionFactory(final Identifier localEndPointId) {
    connectionFactory = networkConnectionService.registerConnectionFactory(connectionFactoryIdentifier,
        metricsMessageCodec, handler, null, localEndPointId);
    return connectionFactory;
  }

  public void unregisterConnectionFactory() {
    networkConnectionService.unregisterConnectionFactory(connectionFactoryIdentifier);
  }

  public ConnectionFactory<MetricsMessage> getConnectionFactory() {
    if (connectionFactory == null) {
      throw new RuntimeException("A connection factory has not been registered yet.");
    }
    return connectionFactory;
  }

  public Identifier getMyId() {
    if (connectionFactory == null) {
      throw new RuntimeException("A connection factory has not been registered yet.");
    }
    return connectionFactory.getLocalEndPointId();
  }
}
