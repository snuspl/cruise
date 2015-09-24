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

import edu.snu.cay.services.rrm.api.ResourceRequestCallbackRouter;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * An implementation of ResourceRequestCallbackRouter.
 * Each evaluatorId has exactly one callback.
 */
public final class ResourceRequestCallbackRouterImpl implements ResourceRequestCallbackRouter {
  private static final Logger LOG = Logger.getLogger(ResourceRequestCallbackRouterImpl.class.getName());

  private final ConcurrentHashMap<String, EventHandler<ActiveContext>> handlerMap =
      new ConcurrentHashMap<>();

  private static final EventHandler<ActiveContext> NOOP_CALLBACK =
      new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          // Do nothing
        }
      };

  @Inject
  private ResourceRequestCallbackRouterImpl() {
  }

  public void register(final String evaluatorId,
                       @Nullable final EventHandler<ActiveContext> callback) {
    if (callback == null) {
      if (handlerMap.putIfAbsent(evaluatorId, NOOP_CALLBACK) != null) {
        LOG.warning("Failed to register NOOP callback for " + evaluatorId + ". Already exists.");
      }
    } else {
      if (handlerMap.putIfAbsent(evaluatorId, callback) != null) {
        LOG.warning("Failed to register callback for " + evaluatorId + ". Already exists.");
      }
    }
  }

  public EventHandler<ActiveContext> getCallback(final String evaluatorId) {
    return handlerMap.remove(evaluatorId);
  }
}
