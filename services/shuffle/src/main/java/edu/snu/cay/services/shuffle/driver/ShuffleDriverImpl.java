/**
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

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.task.ShuffleGroupClient;
import edu.snu.cay.services.shuffle.task.ShuffleContextStartHandler;
import edu.snu.cay.services.shuffle.task.ShuffleContextStopHandler;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation of ShuffleDriver
 */
final class ShuffleDriverImpl implements ShuffleDriver {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;
  private final ConcurrentMap<String, ShuffleGroupManager> managerMap;

  /**
   * Construct a ShuffleDriverImpl
   *
   * @param rootInjector the root injector which injecting the instance of ShuffleDriverImpl
   * @param confSerializer Tang configuration serializer
   */
  @Inject
  private ShuffleDriverImpl(
      final Injector rootInjector,
      final ConfigurationSerializer confSerializer) {
    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.managerMap = new ConcurrentHashMap<>();
  }

  @Override
  public <K extends ShuffleGroupManager> K registerManager(
      final ShuffleGroupDescription shuffleGroupDescription, final Class<K> managerClass) {
    if (managerMap.containsKey(shuffleGroupDescription.getShuffleGroupName())) {
      throw new RuntimeException(shuffleGroupDescription.getShuffleGroupName()
          + " was already registered in ShuffleDriver");
    }
    final Injector forkedInjector = rootInjector.forkInjector();
    forkedInjector.bindVolatileInstance(ShuffleGroupDescription.class, shuffleGroupDescription);
    try {
      final K manager = forkedInjector.getInstance(managerClass);
      managerMap.put(shuffleGroupDescription.getShuffleGroupName(), manager);
      return manager;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K extends ShuffleGroupManager> K getManager(final String shuffleGroupName) {
    return (K) managerMap.get(shuffleGroupName);
  }

  @Override
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, ShuffleContextStartHandler.class)
        .bindSetEntry(ContextStopHandlers.class, ShuffleContextStopHandler.class)
        .build();
  }

  @Override
  public Configuration getTaskConfiguration(final String taskId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final ShuffleGroupManager manager : managerMap.values()) {
      final Configuration clientConf = manager.getClientConfigurationForTask(taskId);

      if (clientConf != null) {
        final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder(clientConf)
            .bindImplementation(ShuffleGroupClient.class, manager.getClientClass())
            .build();
        confBuilder.bindSetEntry(ShuffleParameters.SerializedClientSet.class, confSerializer.toString(conf));
      }
    }
    return confBuilder.build();
  }
}
