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
package edu.snu.cay.services.em.msg.impl;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import org.apache.reef.wake.EventHandler;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A thread-safe implementation of ElasticMemoryCallbackRouter.
 * Each operationId has exactly one callback. Warnings are logged when this does not hold on
 * register or onNext.
 * Each callback is executed within the thread that calls onCompleted.
 */
public final class ElasticMemoryCallbackRouterImpl implements ElasticMemoryCallbackRouter {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryCallbackRouterImpl.class.getName());

  private static final EventHandler<AvroElasticMemoryMessage> NOOP_CALLBACK =
      new EventHandler<AvroElasticMemoryMessage>() {
    @Override
    public void onNext(final AvroElasticMemoryMessage value) {
      // Do nothing
    }
  };

  private final ConcurrentHashMap<String, EventHandler<AvroElasticMemoryMessage>> handlerMap =
      new ConcurrentHashMap<>();

  @Inject
  private ElasticMemoryCallbackRouterImpl() {
  }

  @Override
  public void register(final String operationId, @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
    if (callback == null) {

      if (handlerMap.putIfAbsent(operationId, NOOP_CALLBACK) != null) {
        LOG.warning("Failed to register NOOP callback for " + operationId + ". Already exists.");
      }

    } else {

      if (handlerMap.putIfAbsent(operationId, callback) != null) {
        LOG.warning("Failed to register callback for " + operationId + ". Already exists.");
      }

    }
  }

  @Override
  public void onCompleted(final AvroElasticMemoryMessage msg) {
    if (msg.getOperationId() == null) {
      LOG.warning("No operationId provided. Ignoring msg " + msg);
      return;
    }
    final String operationId = msg.getOperationId().toString();

    final EventHandler<AvroElasticMemoryMessage> handler = handlerMap.remove(operationId);
    if (handler == null) {
      LOG.warning("Failed to find callback for " + operationId + ". Ignoring msg " + msg);
    } else {
      handler.onNext(msg);
    }
  }

  @Override
  public void onFailed(final AvroElasticMemoryMessage msg) {
    if (msg.getOperationId() == null) {
      LOG.log(Level.WARNING, "No operationId provided. Ignoring msg {0}", msg);
      return;
    }
    final String operationId = msg.getOperationId().toString();

    final EventHandler<AvroElasticMemoryMessage> handler = handlerMap.remove(operationId);
    if (handler == null) {
      LOG.log(Level.WARNING, "Failed to find callback for {0}. Ignoring msg {1}", new Object[]{operationId, msg});
    } else {
      handler.onNext(msg);
    }
  }
}
