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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Driver-side shuffle controller.
 * Users can register a ShuffleDescription and retrieve context, task configurations for shuffle components
 * in evaluators through this class.
 *
 * The evaluator-side components can be injected in task or context as a service.
 * // TODO #63: Add more explanation about how both cases are different when the functionality is included.
 */
@DriverSide
@DefaultImplementation(ShuffleDriverImpl.class)
public interface ShuffleDriver {

  /**
   * Register a shuffle which will be handled by a ShuffleManager.
   *
   * @param shuffleDescription a shuffle description
   * @return the ShuffleManager which will handle the registered shuffle.
   */
  <K extends ShuffleManager> K registerShuffle(ShuffleDescription shuffleDescription);

  /**
   * @return context configuration for shuffle service components in evaluators
   */
  Configuration getContextConfiguration();

  /**
   * Return task configuration contains all information about shuffles where the endPointId is included.
   * The returned configuration is used to instantiating Shuffles in the tasks.
   *
   * @param endPointId end point identifier
   * @return task configuration for endPointId
   */
  Configuration getTaskConfiguration(String endPointId);
}
