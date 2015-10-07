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

import edu.snu.cay.services.shuffle.network.TupleNetworkSetup;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// TODO #63: The shuffle configurations can not be injected to the task if the below context already
// have their shuffles.
/**
 * Provide all shuffles that are injected in the same context or task.
 */
public final class ShuffleProvider implements AutoCloseable {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;

  private final Map<String, Shuffle> shuffleMap;

  /**
   * Construct a shuffle provider.
   *
   * @param rootInjector the root injector to share components that are already created
   * @param confSerializer Tang Configuration serializer
   * @param serializedShuffleSet a set of serialized shuffles
   */
  @Inject
  private ShuffleProvider(
      final Injector rootInjector,
      final ConfigurationSerializer confSerializer,
      @Parameter(ShuffleParameters.SerializedShuffleSet.class) final Set<String> serializedShuffleSet) {
    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;

    this.shuffleMap = new HashMap<>();
    for (final String serializedShuffle : serializedShuffleSet) {
      deserializeShuffle(serializedShuffle);
    }
  }

  private void deserializeShuffle(final String serializedShuffle) {
    try {
      final Injector injector = rootInjector.forkInjector(confSerializer.fromString(serializedShuffle));
      injector.getInstance(TupleNetworkSetup.class);
      final Shuffle shuffle = injector.getInstance(Shuffle.class);
      shuffleMap.put(shuffle.getShuffleDescription().getShuffleName(), shuffle);
    } catch (final Exception e) {
      throw new RuntimeException("An exception occurred while deserializing shuffle : " + serializedShuffle, e);
    }
  }

  /**
   * @param shuffleName name of the shuffle
   * @return the Shuffle instance named shuffleName
   */
  public <K, V> Shuffle<K, V>  getShuffle(final String shuffleName) {
    return shuffleMap.get(shuffleName);
  }

  /**
   * Close all shuffles in the provider.
   */
  @Override
  public void close() {
    for (final Shuffle shuffle : shuffleMap.values()) {
      shuffle.close();
    }
  }
}
