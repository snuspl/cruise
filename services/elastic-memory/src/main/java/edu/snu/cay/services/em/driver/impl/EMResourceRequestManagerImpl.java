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

import edu.snu.cay.services.em.driver.api.EMResourceRequestManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * A thread-safe implementation of {@code EMResourceRequestManager}.
 * TODO #188: Support heterogeneous evaluator requests
 */
public final class EMResourceRequestManagerImpl implements EMResourceRequestManager {
  private static final Logger LOG = Logger.getLogger(EMResourceRequestManagerImpl.class.getName());

  /**
   * Remember callbacks to be registered.
   */
  private final BlockingQueue<EventHandler<ActiveContext>> resourceCallbackQueue;

  /**
   * Map of evaluatorId to callback.
   * {@code bindCallback} method will put element and {@code onCompleted} method will remove element.
   */
  private final ConcurrentHashMap<String, EventHandler<ActiveContext>> evalToCallbackMap;

  private static final EventHandler<ActiveContext> NOOP_CALLBACK =
      new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          // Do nothing
        }
      };

  @Inject
  private EMResourceRequestManagerImpl() {
    resourceCallbackQueue = new LinkedBlockingQueue<>();
    evalToCallbackMap = new ConcurrentHashMap<>();
  }

  @Override
  public void register(@Nullable final EventHandler<ActiveContext> callback) {
    if (callback == null) {
      resourceCallbackQueue.add(NOOP_CALLBACK);
    } else {
      resourceCallbackQueue.add(callback);
    }
  }

  /**
   * If there is remembered callback, EM has an outstanding request.
   * If given allocatedEvaluator is EM requested evaluator,
   * put evaluatorId - callback pair in {@code evalToCallbackMap}.
   */
  @Override
  public boolean bindCallback(final AllocatedEvaluator allocatedEvaluator) {
    final EventHandler<ActiveContext> handler = resourceCallbackQueue.poll();
    if (handler == null) {
      return false;
    } else {
      if (evalToCallbackMap.putIfAbsent(allocatedEvaluator.getId(), handler) != null) {
        LOG.warning("Failed to register callback for " + allocatedEvaluator.getId() + ". Already exists.");
      }
      return true;
    }
  }

  @Override
  public void onCompleted(final ActiveContext activeContext) {
    final EventHandler<ActiveContext> handler = evalToCallbackMap.remove(activeContext.getEvaluatorId());
    if (handler == null) {
      LOG.warning("Failed to find callback for " + activeContext.getEvaluatorId()
          + ". Ignoring active context " + activeContext);
    } else {
      handler.onNext(activeContext);
    }
  }
}
