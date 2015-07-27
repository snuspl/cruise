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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.evaluator.impl.EagerPartitionTracker;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for evaluators to notify the driver that they will use a certain partition.
 */
@DefaultImplementation(EagerPartitionTracker.class)
@EvaluatorSide
public interface PartitionTracker {

  /**
   * Send a partition register request to the driver.
   *
   * @param key string that represents a certain data type
   * @param startId minimum value of the id range of the partition to register
   * @param endId maximum value of the id range of the partition to register
   */
  void registerPartition(String key, long startId, long endId);
}
