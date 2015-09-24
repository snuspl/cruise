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
import edu.snu.cay.services.rrm.api.ResourceRequestManager;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * An implementation of ResourceRequestManager.
 * This class currently assumes that every requests have same memory size and number of cores.
 */
@Unit
public final class ResourceRequestManagerImpl implements ResourceRequestManager {
  private static final Logger LOG = Logger.getLogger(ResourceRequestManagerImpl.class.getName());

  private final EvaluatorRequestor requestor;
  private final ResourceRequestCallbackRouter resourceRequestCallbackRouter;

  private final BlockingQueue<Map.Entry<String, EventHandler<ActiveContext>>> requestQueue
      = new LinkedBlockingQueue<>();

  private final ConcurrentHashMap<String, String> idMap = new ConcurrentHashMap<>();

  @Inject
  private ResourceRequestManagerImpl(final EvaluatorRequestor requestor,
                                     final ResourceRequestCallbackRouter resourceRequestCallbackRouter) {
    this.requestor = requestor;
    this.resourceRequestCallbackRouter = resourceRequestCallbackRouter;
  }

  @Override
  public void request(final String requestorId, final int number, final int megaBytes, final int cores,
               @Nullable final EventHandler<ActiveContext> callback) {
    for (int i = 0; i < number; i++) {
      requestQueue.add(new AbstractMap.SimpleEntry<>(requestorId, callback));
    }
    requestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(number)
        .setMemory(megaBytes)
        .setNumberOfCores(cores)
        .build());
  }

  @Override
  public String getRequestor(final AllocatedEvaluator allocatedEvaluator) {
    final Map.Entry<String, EventHandler<ActiveContext>> value = requestQueue.poll();
    if (value == null) {
      LOG.severe("Failed to find a requestor for " + allocatedEvaluator.toString());
      return null;
    } else {
      final String requestorId = value.getKey();
      final EventHandler<ActiveContext> handler = value.getValue();
      final String evaluatorId = allocatedEvaluator.getId();
      if (!idMap.containsKey(evaluatorId)) {
        idMap.put(evaluatorId, requestorId);
        resourceRequestCallbackRouter.register(evaluatorId, handler);
      }
      return idMap.get(evaluatorId);
    }
  }

  @Override
  public void triggerCallback(final ActiveContext activeContext) {
    final EventHandler<ActiveContext> handler =
        resourceRequestCallbackRouter.getCallback(activeContext.getEvaluatorId());
    if (handler == null) {
      LOG.warning("Failed to find callback for " + activeContext.getEvaluatorId()
          + ". Ignoring active context " + activeContext.toString());
    } else {
      handler.onNext(activeContext);
    }
  }

  /**
   * Users can bind this handler in their driver configuration to automatically register callback in router.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      getRequestor(allocatedEvaluator);
    }
  }
}
