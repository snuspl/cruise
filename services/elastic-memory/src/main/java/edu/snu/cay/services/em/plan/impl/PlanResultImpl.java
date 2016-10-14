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
package edu.snu.cay.services.em.plan.impl;

import edu.snu.cay.services.em.plan.api.PlanResult;

/**
 * A plain-old-data implementation of PlanResult.
 */
public final class PlanResultImpl implements PlanResult {
  private final String summary;
  private final int numExecutedOps;

  public PlanResultImpl() {
    this.summary = "The plan result is not specified";
    this.numExecutedOps = -1;
  }

  public PlanResultImpl(final String summary) {
    this.summary = summary;
    this.numExecutedOps = -1;
  }

  public PlanResultImpl(final String summary, final int numExecutedOps) {
    this.summary = summary;
    this.numExecutedOps = numExecutedOps;
  }

  @Override
  public String toString() {
    return "PlanResultImpl{" +
        "summary='" + summary + '\'' +
        ", numExecutedOps=" + numExecutedOps +
        '}';
  }

  @Override
  public int getNumExecutedOps() {
    return numExecutedOps;
  }
}
