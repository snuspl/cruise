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

import edu.snu.cay.services.shuffle.network.ShuffleControlLinkListener;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessageHandler;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

final class ShuffleProviderImpl implements ShuffleProvider {

  private final Injector rootInjector;
  private ConfigurationSerializer confSerializer;
  private final ShuffleControlMessageHandler controlMessageHandler;
  private final ShuffleControlLinkListener controlLinkListener;

  private final Map<String, Shuffle> shuffleMap;

  /**
   * Construct a shuffle provider.
   *
   * @param rootInjector the root injector to share components that are already created
   * @param confSerializer Tang Configuration serializer
   * @param controlMessageHandler a message handler for shuffle control message
   * @param controlLinkListener a link listener for shuffle control message
   * @param serializedShuffleSet a set of serialized shuffles
   * @param idFactory an identifier factory
   * @param networkSetup a shuffle network setup
   * @param endPointId the end point id for the current evaluator
   */
  @Inject
  private ShuffleProviderImpl(
      final Injector rootInjector,
      final ConfigurationSerializer confSerializer,
      final ShuffleControlMessageHandler controlMessageHandler,
      final ShuffleControlLinkListener controlLinkListener,
      @Parameter(ShuffleParameters.SerializedShuffleSet.class) final Set<String> serializedShuffleSet,
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final ShuffleNetworkSetup networkSetup,
      @Parameter(ShuffleParameters.EndPointId.class) final String endPointId) {
    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.controlMessageHandler = controlMessageHandler;
    this.controlLinkListener = controlLinkListener;

    networkSetup.registerConnectionFactories(idFactory.getNewInstance(endPointId));
    this.shuffleMap = new HashMap<>();
    for (final String serializedShuffle : serializedShuffleSet) {
      deserializeShuffle(serializedShuffle);
    }
  }

  private void deserializeShuffle(final String serializedShuffle) {
    try {
      final Configuration shuffleConfiguration = confSerializer.fromString(serializedShuffle);
      final Injector injector = rootInjector.forkInjector(shuffleConfiguration);
      final Shuffle shuffle = injector.getInstance(Shuffle.class);
      final String shuffleName = injector.getNamedInstance(ShuffleParameters.ShuffleName.class);
      shuffleMap.put(shuffleName, shuffle);
      controlMessageHandler.registerMessageHandler(shuffleName, shuffle.getControlMessageHandler());
      controlLinkListener.registerLinkListener(shuffleName, shuffle.getControlLinkListener());
    } catch (final Exception e) {
      throw new RuntimeException("An exception occurred while deserializing shuffle : " + serializedShuffle, e);
    }
  }

  @Override
  public <K, V> Shuffle<K, V>  getShuffle(final String shuffleName) {
    return shuffleMap.get(shuffleName);
  }
}
