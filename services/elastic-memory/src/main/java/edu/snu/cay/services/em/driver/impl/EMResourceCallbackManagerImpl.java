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
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.driver.api.EMResouceCallbackManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * An implementation of {@code EMResourceCallbackManager}.
 */
public final class EMResourceCallbackManagerImpl implements EMResouceCallbackManager {
  private static final Logger LOG = Logger.getLogger(EMResourceCallbackManagerImpl.class.getName());

  /**
   * Map of evaluatorId to callback.
   * {@code register} method will put element and {@code onCompleted} method will remove element.
   */
  private final ConcurrentHashMap<String, EventHandler<ActiveContext>> handlerMap = new ConcurrentHashMap<>();

  private static final EventHandler<ActiveContext> NOOP_CALLBACK =
      new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          // Do nothing
        }
      };

  @Inject
  private EMResourceCallbackManagerImpl() {
  }

  @Override
  public void register(final String evaluatorId) {
    register(evaluatorId, NOOP_CALLBACK);
  }

  @Override
  public void register(final String evaluatorId, final EventHandler<ActiveContext> callback) {
    if (!handlerMap.containsKey(evaluatorId)) {
      handlerMap.put(evaluatorId, callback);
    } else {
      LOG.warning("Failed to register callback for " + evaluatorId + ". Already exists.");
    }
  }

  @Override
  public void onCompleted(final ActiveContext activeContext) {
    if (handlerMap.containsKey(activeContext.getEvaluatorId())) {
      handlerMap.remove(activeContext.getEvaluatorId()).onNext(activeContext);
    } else {
      LOG.warning("Failed to find callback for " + activeContext.getEvaluatorId()
          + ". Ignoring active context " + activeContext.toString());
    }
  }
}
