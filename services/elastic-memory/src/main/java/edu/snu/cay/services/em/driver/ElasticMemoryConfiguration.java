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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.impl.MemoryStoreImpl;
import edu.snu.cay.services.em.msg.ElasticMemoryMsgCodec;
import edu.snu.cay.services.em.ns.NSWrapperConfiguration;
import edu.snu.cay.services.em.ns.NSWrapperContextRegister;
import edu.snu.cay.services.em.ns.NSWrapperParameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.group.impl.driver.ExceptionHandler;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Configuration class for setting evaluator configurations of ElasticMemoryService.
 */
@DriverSide
public final class ElasticMemoryConfiguration {

  private final NSWrapperConfiguration nsWrapperConfiguration;
  private final String driverId;

  @Inject
  private ElasticMemoryConfiguration(final NSWrapperConfiguration nsWrapperConfiguration,
                                     @Parameter(DriverIdentifier.class) final String driverId) {
    this.nsWrapperConfiguration = nsWrapperConfiguration;
    this.driverId = driverId;
  }

  /**
   * Configuration for REEF driver when using Elastic Memory.
   * Binds named parameters for NSWrapper, excluding NameServer-related and default ones.
   * The NameServer will be instantiated at the driver by Tang, and thus NameServer
   * parameters (namely address and port) will be set at runtime by receiving
   * a NameServer injection from Tang.
   *
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NSWrapperParameters.NetworkServiceCodec.class, ElasticMemoryMsgCodec.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServiceHandler.class, ElasticMemoryMsgHandler.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServicePort.class, "0")
        .build();
  }

  /**
   * Configuration for REEF context with Elastic Memory.
   * Elastic Memory requires contexts that communicate through NSWrapper.
   * This configuration binds handlers that register contexts to / unregister
   * contexts from NSWrapper.
   *
   * @return configuration that should be merged with a ContextConfiguration to form a context
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NSWrapperContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NSWrapperContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * Configuration for REEF service with Elastic Memory.
   * Sets up NSWrapper and ElasticMemoryStore, both required for Elastic Memory.
   *
   * @return service configuration that should be passed along with a ContextConfiguration
   */
  public Configuration getServiceConfiguration() {
    final Configuration nsWrapperConf =
        nsWrapperConfiguration.getConfiguration(ElasticMemoryMsgCodec.class,
                                                edu.snu.cay.services.em.evaluator.ElasticMemoryMsgHandler.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, MemoryStoreImpl.class)
        .build();

    final Configuration otherConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, MemoryStoreImpl.class)
        .bindNamedParameter(DriverIdentifier.class, driverId)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, otherConf);
  }
}
