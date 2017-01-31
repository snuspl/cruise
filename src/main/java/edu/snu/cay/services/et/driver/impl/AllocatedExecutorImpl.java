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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Configuration;

/**
 * Implementation for {@link AllocatedExecutor}.
 */
@DriverSide
final class AllocatedExecutorImpl implements AllocatedExecutor {
  private final ActiveContext etContext;
  private final String identifier;

  AllocatedExecutorImpl(final ActiveContext etContext) {
    this.etContext = etContext;
    this.identifier = etContext.getEvaluatorId();
  }

  @Override
  public String getId() {
    return identifier;
  }

  @Override
  public void submitTask(final Configuration taskConf) {
    etContext.submitTask(taskConf);
  }

  @Override
  public void close() {

    // simply close the et context, which is a root context of evaluator.
    // so evaluator(==executor) will be released
    etContext.close();
  }
}
