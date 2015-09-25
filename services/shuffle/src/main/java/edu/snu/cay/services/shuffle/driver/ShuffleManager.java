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

/**
 * Driver-side interface which communicates with corresponding Shuffle instances in evaluators.
 */
@DriverSide
public interface ShuffleManager extends AutoCloseable {

  // TODO #63: The shuffle configurations can not be injected to the task if the below context already
  // have their shuffles.
  /**
   * Return a configuration of the shuffle description for endPointId.
   * Multiple shuffle configurations can be merged and injected to the same context or task,
   * and the merged shuffles are provided through the same ShuffleProvider.
   *
   * @param endPointId end point id
   * @return shuffle configuration
   */
  Configuration getShuffleConfiguration(String endPointId);

  /**
   * @return a shuffle description
   */
  ShuffleDescription getShuffleDescription();

  /**
   * Close resources.
   */
  @Override
  void close();

}
