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

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Driver-side shuffle controller.
 * A ShuffleDescription and a ShuffleManager class who will handle the shuffle are
 * registered through this class.
 */
@DriverSide
@DefaultImplementation(ShuffleDriverImpl.class)
public interface ShuffleDriver {

  /**
   * Register a shuffle which will be handled by a ShuffleManager of managerClass type.
   *
   * @param shuffleDescription shuffle description
   * @param managerClass class of the ShuffleManager
   * @return the registered manager
   */
  <K extends ShuffleManager> K registerShuffle(
      ShuffleDescription shuffleDescription, Class<K> managerClass);

  /**
   * @return context configuration for shuffle service components in tasks
   */
  Configuration getContextConfiguration();

  /**
   * Return task configuration contains all information about shuffles where the task is included.
   * The returned configuration is used to instantiating Shuffles in the tasks.
   *
   * @param taskId task identifier
   * @return task configuration for taskId
   */
  Configuration getTaskConfiguration(String taskId);
}
