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
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@TaskSide
final class ShuffleGroupProviderImpl implements ShuffleGroupProvider {

  private final Injector rootInjector;
  private final ConfigurationSerializer confSerializer;
  private final Map<String, ShuffleGroup> shuffleGroupMap;

  /**
   * @param rootInjector the root injector to inject current instance of DefaultShuffleGroupProviderImpl
   * @param serializedShuffleGroupSet serialized configuration set of ShuffleGroups
   * @param confSerializer Tang configuration serializer
   */
  @Inject
  private ShuffleGroupProviderImpl(
      final Injector rootInjector,
      @Parameter(ShuffleParameters.SerializedShuffleGroupSet.class) final Set<String> serializedShuffleGroupSet,
      final ConfigurationSerializer confSerializer) {

    this.rootInjector = rootInjector;
    this.confSerializer = confSerializer;
    this.shuffleGroupMap = new HashMap<>();
    deserializeShuffleGroupSet(serializedShuffleGroupSet);
  }

  private void deserializeShuffleGroupSet(final Set<String> serializedShuffleGroupSet) {
    for (final String serializedShuffleGroup : serializedShuffleGroupSet) {
      deserializeShuffleGroup(serializedShuffleGroup);
    }
  }

  private void deserializeShuffleGroup(final String serializedShuffleGroup) {
    try {
      final Configuration shuffleGroupConfig = confSerializer.fromString(serializedShuffleGroup);
      final Injector injector = rootInjector.forkInjector(shuffleGroupConfig);
      final ShuffleGroup shuffleGroup = injector.getInstance(ShuffleGroup.class);
      final ShuffleGroupDescription description = shuffleGroup.getShuffleGroupDescription();
      shuffleGroupMap.put(description.getShuffleGroupName(), shuffleGroup);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing shuffle group "
          + serializedShuffleGroup, exception);
    }
  }

  @Override
  public ShuffleGroup getShuffleGroup(final String shuffleGroupName) {
    return shuffleGroupMap.get(shuffleGroupName);
  }
}
