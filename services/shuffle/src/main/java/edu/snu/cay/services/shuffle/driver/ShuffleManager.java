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

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

/**
 * Driver-side interface which communicates with corresponding Shuffle instances in evaluators.
 */
@DriverSide
public interface ShuffleManager {

  /**
   * Return a configuration of the shuffle description for the evaluator named evaluatorId.
   * It returns Optional.empty if the evaluatorId is not included in any shuffles.
   *
   * @param evaluatorId the id of the evaluator
   * @return optional shuffle configuration for the node
   */
  Optional<Configuration> getShuffleConfiguration(String evaluatorId);

  /**
   * @return a shuffle description handled by the ShuffleManager
   */
  ShuffleDescription getShuffleDescription();
}
