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
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * An implementation of ResourceRequestManager.
 * Since we cannot put tags on EvaluatorRequest, we should distinguish
 * an evaluator from others only using memory size and number of cores.
 * This implementation currently assumes that every requests have same memory size and number of cores.
 */
@Unit
public final class ResourceRequestManagerImpl implements ResourceRequestManager {
  private static final Logger LOG = Logger.getLogger(ResourceRequestManagerImpl.class.getName());

  /**
   * REEF evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * A queue storing requestorId and callback.
   * {@code request} puts an element in this queue,
   * and {@code getRequestor} polls an element from this queue.
   */
  private final BlockingQueue<Map.Entry<String, EventHandler<ActiveContext>>> requestQueue
      = new LinkedBlockingQueue<>();

  /**
   * Map of evaluatorId to requestorId.
   * {@code getRequestor} puts an element in this map.
   */
  private final ConcurrentHashMap<String, String> idMap = new ConcurrentHashMap<>();

  /**
   * Map of evaluatorId to callback.
   * Contains a callback popped out from {@code requestQueue}.
   */
  private final ConcurrentHashMap<String, EventHandler<ActiveContext>> handlerMap =
      new ConcurrentHashMap<>();

  /**
   * A no operation callback.
   * When user calls {@code request} without EventHandler parameter, this object will be registered.
   */
  private static final EventHandler<ActiveContext> NOOP_CALLBACK =
      new EventHandler<ActiveContext>() {
        @Override
        public void onNext(final ActiveContext activeContext) {
          // Do nothing
        }
      };

  @Inject
  private ResourceRequestManagerImpl(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  /**
   * Request evaluators without callback.
   * One method call may requests for more than one evaluators.
   */
  @Override
  public void request(final String requestorId, final EvaluatorRequest evaluatorRequest) {
    request(requestorId, evaluatorRequest, NOOP_CALLBACK);
  }

  /**
   * Request evaluators with a specified callback.
   * One method call may requests for more than one evaluators.
   * Stores requestorId and callback in {@code requestQueue}.
   */
  @Override
  public void request(final String requestorId, final EvaluatorRequest evaluatorRequest,
                      final EventHandler<ActiveContext> callback) {
    for (int i = 0; i < evaluatorRequest.getNumber(); i++) {
      requestQueue.add(new AbstractMap.SimpleEntry<>(requestorId, callback));
    }
    requestor.submit(evaluatorRequest);
  }

  /**
   * Figure out which component requested given evaluator.
   * Current implementation assumes only homogeneous evaluator requests,
   * so the first element in {@code requestQueue} is regared as the requestor of the evaluator.
   * Puts a pair of evaluatorId and requestorId into {@code idMap}
   * and a pair of evaluatorId and callback into {@code handlerMap}.
   * If given evaluator already has a corresponding requestor,
   * this method do not change queue and maps.
   */
  @Override
  public String getRequestor(final AllocatedEvaluator allocatedEvaluator) {
    final String evaluatorId = allocatedEvaluator.getId();
    if (!idMap.containsKey(evaluatorId)) {
      final Map.Entry<String, EventHandler<ActiveContext>> value = requestQueue.poll();
      if (value == null) {
        LOG.severe("Failed to find a requestor for " + allocatedEvaluator.toString());
        return null;
      } else {
        final String requestorId = value.getKey();
        final EventHandler<ActiveContext> handler = value.getValue();
        idMap.put(evaluatorId, requestorId);
        handlerMap.put(evaluatorId, handler);
        return requestorId;
      }
    } else {
      return idMap.get(evaluatorId);
    }
  }

  /**
   * Figure out which component requested given evaluator.
   * This method do not touch queue and maps,
   * only reads {@code idMap} and returns corresponding requestorId.
   */
  @Override
  public String getRequestor(final String evaluatorId) {
    if (!idMap.containsKey(evaluatorId)) {
      LOG.severe("Failed to find a requestor for " + evaluatorId);
      return null;
    } else {
      return idMap.get(evaluatorId);
    }
  }

  /**
   * Remove a evaluatorId-callback pair corresponding to
   * given ActiveContext's evaluatorId from {@code handlerMap}.
   */
  @Override
  public void triggerCallback(final ActiveContext activeContext) {
    final EventHandler<ActiveContext> handler = handlerMap.remove(activeContext.getEvaluatorId());
    if (handler == null) {
      LOG.warning("Failed to find callback for " + activeContext.getEvaluatorId()
          + ". Ignoring active context " + activeContext.toString());
    } else {
      handler.onNext(activeContext);
    }
  }

  /**
   * A handler automatically registers evaluator in {@code idMap} and {@code handlerMap}.
   * A user who does not call {@code getRequestor} in AllocatedEvaluatorHandler
   * may use {@code getRequestor} properly in other handlers due to this object.
   */
  public final class RegisterEvaluatorHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      getRequestor(allocatedEvaluator);
    }
  }
}
