/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.plan.api;

import org.apache.reef.util.Optional;

/**
 * An interface class that represents a fine-grained step of a plan.
 */
public interface PlanOperation {
  /**
   * @return a namespace of operation
   */
  String getNamespace();

  /**
   * @return a type of operation
   */
  String getOpType();

  /**
   * @return a target evaluator id. For MOVE operation, it returns an empty Optional.
   */
  Optional<String> getEvalId();

  /**
   * @return an Optional with the TransferStep if the operation type is EM's MOVE.
   * For other operations, it returns an empty Optional.
   */
  Optional<TransferStep> getTransferStep();
}
