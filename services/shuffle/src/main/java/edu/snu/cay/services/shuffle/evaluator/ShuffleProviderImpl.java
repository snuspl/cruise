/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cay.services.shuffle.evaluator;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class ShuffleProviderImpl implements ShuffleProvider {

  private final Injector rootInjector;
  private ConfigurationSerializer confSerializer;

  private final Map<String, Shuffle> shuffleMap;

  @Inject
  private ShuffleProviderImpl(
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
      final Configuration shuffleConfiguration = confSerializer.fromString(serializedShuffle);
      final Injector injector = rootInjector.forkInjector(shuffleConfiguration);
      final Shuffle shuffle = injector.getInstance(Shuffle.class);
      shuffleMap.put(shuffle.getShuffleDescription().getShuffleName(), shuffle);
    } catch (final Exception e) {
      throw new RuntimeException("An exception occurred while deserializing shuffle : " + serializedShuffle, e);
    }
  }

  @Override
  public <K, V> Shuffle<K, V>  getShuffle(final String shuffleName) {
    return shuffleMap.get(shuffleName);
  }
}
