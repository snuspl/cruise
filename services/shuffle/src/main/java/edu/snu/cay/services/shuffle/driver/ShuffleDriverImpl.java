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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.ShuffleControlLinkListener;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessageCodec;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessageHandler;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.evaluator.ShuffleContextStopHandler;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class ShuffleDriverImpl implements ShuffleDriver {

  private final Class<? extends ShuffleManager> shuffleManagerClass;
  private final ConfigurationSerializer confSerializer;
  private final Injector rootInjector;
  private final ShuffleControlMessageHandler controlMessageHandler;
  private final ShuffleControlLinkListener controlLinkListener;

  private final ConcurrentMap<String, ShuffleManager> managerMap;

  /**
   * @param confSerializer Tang configuration serializer
   */
  @Inject
  private ShuffleDriverImpl(
      @Parameter(ShuffleParameters.ShuffleManagerClassName.class) final String shuffleManagerClassName,
      final ConfigurationSerializer confSerializer,
      final Injector rootInjector,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService,
      final ShuffleControlMessageCodec controlMessageCodec,
      final ShuffleControlMessageHandler controlMessageHandler,
      final ShuffleControlLinkListener controlLinkListener) {
    try {
      this.shuffleManagerClass = (Class<? extends ShuffleManager>) Class.forName(shuffleManagerClassName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.confSerializer = confSerializer;
    this.rootInjector = rootInjector;
    this.controlMessageHandler = controlMessageHandler;
    this.controlLinkListener = controlLinkListener;

    final Identifier controlMessageNetworkId = idFactory.getNewInstance(
        ShuffleParameters.SHUFFLE_CONTROL_MSG_NETWORK_ID);
    try {
      networkConnectionService.registerConnectionFactory(
          controlMessageNetworkId, controlMessageCodec, controlMessageHandler, controlLinkListener);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }
    this.managerMap = new ConcurrentHashMap<>();
  }

  @Override
  public <K extends ShuffleManager> K registerShuffle(final ShuffleDescription shuffleDescription) {
    if (managerMap.containsKey(shuffleDescription.getShuffleName())) {
      throw new RuntimeException(shuffleDescription.getShuffleName()
          + " was already registered in ShuffleDriver");
    }
    final Injector injector = rootInjector.forkInjector();
    injector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
    try {
      final K manager = (K)injector.getInstance(shuffleManagerClass);
      final String shuffleName = shuffleDescription.getShuffleName();
      managerMap.put(shuffleName, manager);
      controlMessageHandler.registerMessageHandler(shuffleName, manager.getControlMessageHandler());
      controlLinkListener.registerLinkListener(shuffleName, manager.getControlLinkListener());
      return manager;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStopHandlers.class, ShuffleContextStopHandler.class)
        .build();
  }

  @Override
  public Configuration getTaskConfiguration(final String endPointId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final ShuffleManager manager : managerMap.values()) {
      final Optional<Configuration> shuffleConf = manager.getShuffleConfiguration(endPointId);

      if (shuffleConf.isPresent()) {
        // The shuffle manager has the endPointId as a sender or a receiver.
        confBuilder.bindSetEntry(
            ShuffleParameters.SerializedShuffleSet.class, confSerializer.toString(shuffleConf.get()));
        confBuilder.bindNamedParameter(ShuffleParameters.EndPointId.class, endPointId);
      }
    }
    return confBuilder.build();
  }
}
