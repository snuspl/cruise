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
package edu.snu.cay.dolphin.async.plan.impl;

import edu.snu.cay.dolphin.async.plan.api.Plan;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;

import java.util.Collection;
import java.util.Collections;

/**
 * An empty plan.
 */
public final class EmptyPlan implements Plan {

  /**
   * Constructs an empty plan involving no operation.
   */
  public EmptyPlan() {
  }

  @Override
  public Collection<String> getEvaluatorsToAdd(final String namespace) {
    return Collections.emptySet();
  }

  @Override
  public Collection<String> getEvaluatorsToDelete(final String namespace) {
    return Collections.emptySet();
  }

  @Override
  public Collection<TransferStep> getTransferSteps(final String namespace) {
    return Collections.emptySet();
  }

  @Override
  public String toString() {
    return "EmptyPlan";
  }
}
