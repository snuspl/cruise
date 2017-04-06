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
package edu.snu.cay.common.centcomm;

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.ns.NetworkDriverRegister;
import edu.snu.cay.common.centcomm.params.CentCommClientHandlers;
import edu.snu.cay.common.centcomm.params.CentCommClientInfo;
import edu.snu.cay.common.centcomm.params.SerializedCentCommSlavesConf;
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
 * Configuration class for CeneComm Service.
 * Provides configuration for REEF driver.
 * Current implementation assumes that the driver is the master of Cent Comm Service.
 * A client of CentComm Service is a user of this service, which is different from REEF client.
 */
@ClientSide
public final class CentCommConf {

  /**
   * Names of the clients of CentComm Service.
   */
  private final List<String> centCommClientNames;

  /**
   * Master-side message handlers for each client.
   */
  private final List<Class<? extends EventHandler<CentCommMsg>>> masterSideMsgHandlers;

  /**
   * Slave-side message handlers for each client.
   */
  private final List<Class<? extends EventHandler<CentCommMsg>>> slaveSideMsgHandlers;

  private CentCommConf(
      final List<String> centCommClientNames,
      final List<Class<? extends EventHandler<CentCommMsg>>> masterSideMsgHandlers,
      final List<Class<? extends EventHandler<CentCommMsg>>> slaveSideMsgHandlers) {
    this.centCommClientNames = centCommClientNames;
    this.masterSideMsgHandlers = masterSideMsgHandlers;
    this.slaveSideMsgHandlers = slaveSideMsgHandlers;
  }

  /**
   * Configuration for REEF driver when using Cent Comm Service.
   * Binds NetworkConnectionService registration handlers.
   * @return configuration that should be submitted with a DriverConfiguration
   */
  // TODO #352: After REEF-402 is resolved, we should use {@code JavaConfigurationBuilder.bindList()} to bind handlers.
  public Configuration getDriverConfiguration() {
    final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
    final JavaConfigurationBuilder commonConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final JavaConfigurationBuilder driverConfBuilder = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverStartHandler.class, NetworkDriverRegister.RegisterDriverHandler.class);
    final JavaConfigurationBuilder slaveConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();

    // To match clients' name and handlers, encode the class names of handlers and the client name
    // using delimiter "//" which cannot be included in Java class name.
    for (int i = 0; i < centCommClientNames.size(); i++) {
      commonConfBuilder.bindSetEntry(CentCommClientInfo.class,
          String.format("%s//%s//%s",
              centCommClientNames.get(i),
              masterSideMsgHandlers.get(i).getName(),
              slaveSideMsgHandlers.get(i).getName()));
      driverConfBuilder.bindSetEntry(CentCommClientHandlers.class, masterSideMsgHandlers.get(i));
      slaveConfBuilder.bindSetEntry(CentCommClientHandlers.class, slaveSideMsgHandlers.get(i));
    }

    final Configuration commonConf = commonConfBuilder.build();
    final String serializedSlaveConf
        = confSerializer.toString(Configurations.merge(commonConf, slaveConfBuilder.build()));
    driverConfBuilder.bindNamedParameter(SerializedCentCommSlavesConf.class, serializedSlaveConf);
    return Configurations.merge(commonConf, driverConfBuilder.build());
  }

  /**
   * @return new builder for {@link CentCommConf}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder implements org.apache.reef.util.Builder<CentCommConf> {
    private Map<String, Pair<Class<? extends EventHandler<CentCommMsg>>,
        Class<? extends EventHandler<CentCommMsg>>>> centCommClients = new HashMap<>();

    /**
     * Add a new client of CentComm Service.
     * @param clientName name of CentComm Service client, used to identify messages from different clients
     * @param masterSideMsgHandler message handler for CentComm master
     * @param slaveSideMsgHandler message handler for CentComm slave
     * @return Builder
     */
    public Builder addCentCommClient(final String clientName,
                                     final Class<? extends EventHandler<CentCommMsg>> masterSideMsgHandler,
                                     final Class<? extends EventHandler<CentCommMsg>> slaveSideMsgHandler) {
      this.centCommClients.put(clientName,
          new Pair<>(masterSideMsgHandler, slaveSideMsgHandler));
      return this;
    }

    @Override
    public CentCommConf build() {
      final List<String> centCommClientNames = new ArrayList<>(centCommClients.size());
      final List<Class<? extends EventHandler<CentCommMsg>>> centCommMasterHandlers
          = new ArrayList<>(centCommClients.size());
      final List<Class<? extends EventHandler<CentCommMsg>>> centCommSlaveHandlers
          = new ArrayList<>(centCommClients.size());
      for (final String key : centCommClients.keySet()) {
        centCommClientNames.add(key);
        centCommMasterHandlers.add(centCommClients.get(key).getFirst());
        centCommSlaveHandlers.add(centCommClients.get(key).getSecond());
      }
      return new CentCommConf(centCommClientNames, centCommMasterHandlers, centCommSlaveHandlers);
    }
  }
}
