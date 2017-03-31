/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.impl.MessageHandlerImpl;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.IdentifierFactory;

/**
 * A builder for configuration required for launching executor.
 */
@Private
public final class ExecutorServiceConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> ET_IDENTIFIER = new RequiredParameter<>();
  public static final RequiredParameter<String> IDENTIFIER = new RequiredParameter<>();

  /**
   * Parameters required for NameResolverConfiguration.
   */
  public static final RequiredParameter<String> NAME_SERVICE_HOST = new RequiredParameter<>();
  public static final RequiredParameter<Integer> NAME_SERVICE_PORT = new RequiredParameter<>();

  /**
   * Parameters required for identification.
   */
  public static final RequiredImpl<IdentifierFactory> IDENTIFIER_FACTORY = new RequiredImpl<>();
  public static final RequiredParameter<String> DRIVER_IDENTIFIER = new RequiredParameter<>();

  /**
   * ConfigurationModule.
   */
  public static final ConfigurationModule CONF = new ExecutorServiceConfiguration()
      .bindNamedParameter(ETIdentifier.class, ET_IDENTIFIER)
      .bindNamedParameter(ExecutorIdentifier.class, IDENTIFIER)
      .bindNamedParameter(NameResolverNameServerAddr.class, NAME_SERVICE_HOST)
      .bindNamedParameter(NameResolverNameServerPort.class, NAME_SERVICE_PORT)
      .bindImplementation(IdentifierFactory.class, IDENTIFIER_FACTORY)
      .bindImplementation(MessageHandler.class, MessageHandlerImpl.class)
      .bindNamedParameter(DriverIdentifier.class, DRIVER_IDENTIFIER)
      .build();
}
