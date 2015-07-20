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
import edu.snu.cay.services.shuffle.task.ShuffleGroupClient;
import org.apache.reef.tang.Configuration;

/**
 * Manage a shuffle group with corresponding ShuffleGroupClients in tasks
 */
public interface ShuffleGroupManager {

  /**
   * Return configuration for ShuffleClient in task named taskId.
   * It returns null if the shuffle group managed by ShuffleManager
   * does not have some shuffles contain taskId as sender or receiver
   *
   * @param taskId task identifier
   * @return client configuration for task
   */
  Configuration getClientConfigurationForTask(String taskId);

  /**
   * Return ShuffleClient class communicating with ShuffleManager to control shuffle group
   *
   * @return class of ShuffleClient
   */
  Class<? extends ShuffleGroupClient> getClientClass();

  /**
   * @return description about shuffle group handled by the shuffle group manager
   */
  ShuffleGroupDescription getShuffleGroupDescription();
}
