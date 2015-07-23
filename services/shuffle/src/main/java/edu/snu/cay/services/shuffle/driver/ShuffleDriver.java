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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Driver-side shuffle controller.
 * Shuffle groups and the corresponding group manager classes are registered
 * through this class.
 */
@DriverSide
@DefaultImplementation(ShuffleDriverImpl.class)
public interface ShuffleDriver {

  /**
   * Register a shuffle group which will be handled by a ShuffleGroupManager of managerClass type.
   * The ShuffleGroupManager class should have an injectable constructor for Tang injection.
   *
   * @param shuffleGroupDescription shuffle group description
   * @param managerClass class of the ShuffleGroupManager
   * @return the registered manager
   */
  <K extends ShuffleGroupManager> K registerManager(
      ShuffleGroupDescription shuffleGroupDescription, Class<K> managerClass);

  /**
   * @return context configuration for shuffle service components in tasks
   */
  Configuration getContextConfiguration();

  /**
   * Return task configuration contains all information about shuffle groups where the task is included.
   * The returned configuration is used to instantiating ShuffleGroups in the task.
   *
   * @param taskId task identifier
   * @return task configuration for taskId
   */
  Configuration getTaskConfiguration(String taskId);
}
