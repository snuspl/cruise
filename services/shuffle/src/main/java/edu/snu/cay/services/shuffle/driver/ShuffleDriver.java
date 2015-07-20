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
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Main Driver side shuffle controller. The shuffle group is registered through this class and users
 * can determine what implementation of shuffle group manager should handle the shuffle group.
 */
@DefaultImplementation(ShuffleDriverImpl.class)
public interface ShuffleDriver {

  /**
   * Register shuffle group which will be handled by the ShuffleGroupManager with managerClass type.
   * The ShuffleGroupManager class should have an injectable constructor since the manager is
   * instantiated by Tang injector.
   *
   * @param shuffleGroupDescription the shuffle group description will be handled by registered shuffle group manager
   * @param managerClass class extending ShuffleGroupManager
   * @param <K> type of ShuffleGroupManager
   * @return the manager handles registered shuffle group
   */
  <K extends ShuffleGroupManager> K registerManager(
      ShuffleGroupDescription shuffleGroupDescription, Class<K> managerClass);

  /**
   * Get ShuffleGroupManager managing shuffle group named shuffleGroupName
   *
   * @param shuffleGroupName the name of shuffle group
   * @param <K> type of ShuffleManager
   * @return the manager which handles shuffle group named shuffleGroupName
   */
  <K extends ShuffleGroupManager> K getManager(String shuffleGroupName);

  /**
   * Return context configuration for shuffle service in task
   *
   * @return context configuration
   */
  Configuration getContextConfiguration();

  /**
   * Return task configuration contains all information about shuffle groups where the task is included.
   * The returned configuration is used to instantiating ShuffleGroupClients in the task.
   *
   * @param taskId task identifier
   * @return task configuration for taskId
   */
  Configuration getTaskConfiguration(String taskId);
}
