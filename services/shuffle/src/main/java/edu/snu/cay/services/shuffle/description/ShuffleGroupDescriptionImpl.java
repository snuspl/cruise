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
package edu.snu.cay.services.shuffle.description;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.*;

/**
 * Default implementation of ShuffleGroupDescription, it can be instantiated using Builder or Tang injector.
 */
public final class ShuffleGroupDescriptionImpl implements ShuffleGroupDescription {

  private final String shuffleGroupName;
  private final Map<String, ShuffleDescription> shuffleDescriptionMap;
  private final List<String> shuffleNameList;

  @Inject
  private ShuffleGroupDescriptionImpl(
      @Parameter(ShuffleParameters.ShuffleGroupName.class) final String shuffleGroupName,
      @Parameter(ShuffleParameters.SerializedShuffleSet.class) final Set<String> serializedShuffleSet,
      final ConfigurationSerializer confSerializer) {
    this.shuffleGroupName = shuffleGroupName;
    this.shuffleDescriptionMap = new HashMap<>();
    this.shuffleNameList = new ArrayList<>();
    for (final String serializedShuffle : serializedShuffleSet) {
      deserializeShuffle(serializedShuffle, confSerializer);
    }

    // Set does not guarantee that the order of elements is preserved, we should at least sort the receiver ids.
    Collections.sort(shuffleNameList);
  }

  private void deserializeShuffle(final String serializedShuffle, final ConfigurationSerializer confSerializer) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(confSerializer.fromString(serializedShuffle));
      final ShuffleDescription description = injector.getInstance(ShuffleDescription.class);
      final String shuffleName = description.getShuffleName();
      shuffleDescriptionMap.put(shuffleName, description);
      shuffleNameList.add(shuffleName);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing shuffle " + serializedShuffle, exception);
    }
  }

  private ShuffleGroupDescriptionImpl(
      final String shuffleGroupName,
      final Map<String, ShuffleDescription> shuffleDescriptionMap,
      final List<String> shuffleNameList) {
    this.shuffleGroupName = shuffleGroupName;
    this.shuffleDescriptionMap = shuffleDescriptionMap;
    this.shuffleNameList = shuffleNameList;
    Collections.sort(shuffleNameList);
  }

  @Override
  public String getShuffleGroupName() {
    return shuffleGroupName;
  }

  @Override
  public List<String> getShuffleNameList() {
    return shuffleNameList;
  }

  @Override
  public ShuffleDescription getShuffleDescription(final String shuffleName) {
    return shuffleDescriptionMap.get(shuffleName);
  }

  public static Builder newBuilder(final String shuffleGroupName) {
    return new Builder(shuffleGroupName);
  }

  public static final class Builder {

    private final String shuffleGroupName;
    private final Map<String, ShuffleDescription> shuffleDescriptionMap;
    private final List<String> shuffleNameList;

    private Builder(final String shuffleGroupName) {
      this.shuffleGroupName = shuffleGroupName;
      this.shuffleDescriptionMap = new HashMap<>();
      this.shuffleNameList = new ArrayList<>();
    }

    public Builder addShuffle(final ShuffleDescription shuffleDescription) {
      if (shuffleDescriptionMap.containsKey(shuffleDescription.getShuffleName())) {
        throw new RuntimeException(shuffleDescription.getShuffleName() + " was already added.");
      }

      shuffleDescriptionMap.put(shuffleDescription.getShuffleName(), shuffleDescription);
      shuffleNameList.add(shuffleDescription.getShuffleName());
      return this;
    }

    public ShuffleGroupDescriptionImpl build() {
      return new ShuffleGroupDescriptionImpl(
          shuffleGroupName,
          shuffleDescriptionMap,
          shuffleNameList
      );
    }
  }
}
