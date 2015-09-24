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
package edu.snu.cay.services.rrm.impl;

import edu.snu.cay.services.rrm.api.ResourceRequestManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;

public class ResourceRequestManagerImpl implements ResourceRequestManager {

  @Inject
  private ResourceRequestManagerImpl() {

  }

  @Override
  void request(String requestorId, int number, int megaBytes, int cores,
               @Nullable EventHandler<ActiveContext> callback) {

  }

  @Override
  String getRequestor(AllocatedEvaluator allocatedEvaluator) {

  }

  @Override
  void triggerCallback(ActiveContext activeContext) {

  }
}
