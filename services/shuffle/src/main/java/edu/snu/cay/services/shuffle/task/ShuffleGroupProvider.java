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

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Task-side interface for providing ShuffleGroup.
 *
 * ShuffleGroups in the provider are automatically injected if the task configuration
 * was merged with proper configuration from ShuffleDriver.
 */
@TaskSide
@DefaultImplementation(ShuffleGroupProviderImpl.class)
public interface ShuffleGroupProvider {

  /**
   * @param shuffleGroupName name of the shuffle group
   * @return the shuffle group named shuffleGroupName
   */
  ShuffleGroup getShuffleGroup(String shuffleGroupName);

}
