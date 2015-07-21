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
import edu.snu.cay.services.shuffle.task.ShuffleGroup;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;

/**
 * Manage a shuffle group with corresponding ShuffleGroups in tasks.
 */
@DriverSide
public interface ShuffleGroupManager {

  /**
   * Return configuration for ShuffleGroup of task named taskId.
   * It returns null if the taskId is not included in any shuffles.
   *
   * @param taskId task identifier
   * @return shuffle group configuration for task
   */
  Configuration getShuffleGroupConfigurationForTask(String taskId);

  /**
   * Return ShuffleGroup class that is communicating with ShuffleGroupManager
   *
   * @return class of ShuffleGroup
   */
  Class<? extends ShuffleGroup> getShuffleGroupClass();

  /**
   * @return description about shuffle group handled by the ShuffleGroupManager
   */
  ShuffleGroupDescription getShuffleGroupDescription();
}
