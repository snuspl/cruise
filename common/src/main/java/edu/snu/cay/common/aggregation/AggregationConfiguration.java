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
package edu.snu.cay.common.aggregation;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.driver.parameters.DriverStartHandler;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration class for Aggregation Service.
 * Provides configuration for REEF driver.
 * Current implementation assumes that the driver is the master of Aggregation Service.
 * A client of Aggregation Service is a user of this service, which is different from REEF client.
 */
@ClientSide
public final class AggregationConfiguration {

  /**
   * Names of the clients of Aggregation Service.
   */
  private final List<String> aggregationClientNames;

  /**
   * Master-side handlers for each clients.
   */
  private final List<Class<? extends EventHandler<AggregationMessage>>> aggregationMasterHandlers;

  /**
   * Slave-side handlers for each clients.
   */
  private final List<Class<? extends EventHandler<AggregationMessage>>> aggregationSlaveHandlers;

  private AggregationConfiguration(
      final List<String> aggregationClientNames,
      final List<Class<? extends EventHandler<AggregationMessage>>> aggregationMasterHandlers,
      final List<Class<? extends EventHandler<AggregationMessage>>> aggregationSlaveHandlers) {
    this.aggregationClientNames = aggregationClientNames;
    this.aggregationMasterHandlers = aggregationMasterHandlers;
    this.aggregationSlaveHandlers = aggregationSlaveHandlers;
  }

  /**
   * Configuration for REEF driver when using Aggregation Service.
   * Binds NetworkConnectionService registration handlers.
   * After REEF-402 is resolved, we should use
   * {@code JavaConfigurationBuilder.bindList()} to bind handlers.
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public Configuration getDriverConfiguration() {
    final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
    final JavaConfigurationBuilder commonConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final JavaConfigurationBuilder driverConfBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverStartHandler.class, NetworkDriverRegister.RegisterDriverHandler.class);
    final JavaConfigurationBuilder slaveConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    // To match clients' name and handlers, encode the class names of handlers and the client name
    // using delimiter "//" which cannot be included in Java class name.
    for (int i = 0; i < aggregationClientNames.size(); i++) {
      commonConfBuilder.bindSetEntry(AggregationClientInfo.class,
          String.format("%s//%s//%s",
              aggregationClientNames.get(i),
              aggregationMasterHandlers.get(i).getName(),
              aggregationSlaveHandlers.get(i).getName()));
      driverConfBuilder.bindSetEntry(AggregationClientHandlers.class, aggregationMasterHandlers.get(i));
      slaveConfBuilder.bindSetEntry(AggregationClientHandlers.class, aggregationSlaveHandlers.get(i));
    }

    final Configuration commonConf = commonConfBuilder.build();
    final String serializedSlaveConf
        = confSerializer.toString(Configurations.merge(commonConf, slaveConfBuilder.build()));
    driverConfBuilder.bindNamedParameter(AggregationSlaveSerializedConf.class, serializedSlaveConf);
    return Configurations.merge(commonConf, driverConfBuilder.build());
  }

  /**
   * @return new builder for {@link AggregationConfiguration}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<AggregationConfiguration> {
    private Map<String, Pair<Class<? extends EventHandler<AggregationMessage>>,
        Class<? extends EventHandler<AggregationMessage>>>> aggregationClients = new HashMap<>();

    /**
     * Add a new client of Aggregation Service.
     * @param clientName name of Aggregation Service client, used to identify messages from different clients
     * @param masterHandler message handler for aggregation master
     * @param slaveHandler message handler for aggregation slave
     * @return Builder
     */
    public Builder addAggregationClient(final String clientName,
                                        final Class<? extends EventHandler<AggregationMessage>> masterHandler,
                                        final Class<? extends EventHandler<AggregationMessage>> slaveHandler) {
      this.aggregationClients.put(clientName,
          new Pair<Class<? extends EventHandler<AggregationMessage>>,
              Class<? extends EventHandler<AggregationMessage>>>(masterHandler, slaveHandler));
      return this;
    }

    @Override
    public AggregationConfiguration build() {
      final List<String> aggregationClientNames = new ArrayList<>(aggregationClients.size());
      final List<Class<? extends EventHandler<AggregationMessage>>> aggregationMasterHandlers
          = new ArrayList<>(aggregationClients.size());
      final List<Class<? extends EventHandler<AggregationMessage>>> aggregationSlaveHandlers
          = new ArrayList<>(aggregationClients.size());
      for (final String key : aggregationClients.keySet()) {
        aggregationClientNames.add(key);
        aggregationMasterHandlers.add(aggregationClients.get(key).getFirst());
        aggregationSlaveHandlers.add(aggregationClients.get(key).getSecond());
      }
      return new AggregationConfiguration(aggregationClientNames, aggregationMasterHandlers, aggregationSlaveHandlers);
    }
  }
}
