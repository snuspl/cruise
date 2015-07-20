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
package edu.snu.cay.services.shuffle.task;

import edu.snu.cay.services.shuffle.description.ShuffleGroupDescription;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of ShuffleService
 */
final class ShuffleServiceImpl implements ShuffleService {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;
  private final Map<String, ShuffleGroupClient> clientMap;

  /**
   * Construct a ShuffleServiceImpl

   * @param rootInjector the root injector which injecting the instance of ShuffleServiceImpl
   * @param serializedClientSet serialized configuration set for ShuffleGroupClient
   * @param confSerializer Tang configuration serializer
   */
  @Inject
  private ShuffleServiceImpl(
      final Injector rootInjector,
      @Parameter(ShuffleParameters.SerializedClientSet.class) final Set<String> serializedClientSet,
      final ConfigurationSerializer confSerializer) {

    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.clientMap = new ConcurrentHashMap<>();
    deserializeClientSet(serializedClientSet);
  }

  private void deserializeClientSet(final Set<String> serializedClientSet) {
    for (final String serializedClient : serializedClientSet) {
      deserializeClient(serializedClient);
    }
  }

  private void deserializeClient(final String serializedClient) {
    try {
      final Configuration clientConfig = confSerializer.fromString(serializedClient);
      final Injector injector = rootInjector.forkInjector(clientConfig);
      final ShuffleGroupClient client = injector.getInstance(ShuffleGroupClient.class);
      final ShuffleGroupDescription description = client.getShuffleGroupDescription();
      clientMap.put(description.getShuffleGroupName(), client);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing client "
          + serializedClient, exception);
    }
  }

  @Override
  public ShuffleGroupClient getClient(final String shuffleGroupName) {
    return clientMap.get(shuffleGroupName);
  }
}
