/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.common.impl;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes callbacks for operations by their event type and unique identifier.
 * Registered callbacks are invoked when they are completed or failed.
 */
@Private
public final class CallbackRegistry {

  private final Map<Class, Map<String, EventHandler>> callbackMapsPerType = new ConcurrentHashMap<>();

  @Inject
  private CallbackRegistry() {

  }

  /**
   * Registers a new callback for an EM operation.
   * @param eventType a type of event
   * @param identifier A unique ID for the operation.
   * @param callback The handler to be called when operation is complete, or null for a no-op callback.
   * @param <T> a type of event
   */
  public synchronized <T> void register(final Class<? extends T> eventType,
                                        final String identifier,
                                        final EventHandler<T> callback) {
    final Map<String, EventHandler> callbackMaps =
        callbackMapsPerType.computeIfAbsent(eventType, k -> new ConcurrentHashMap<>());

    if (callbackMaps.put(identifier, callback) != null) {
      throw new RuntimeException(String.format("Callback for %s has been already registered", identifier));
    }
  }

  /**
   * Calls the registered callback for a completed operation.
   * @param eventType a type of event
   * @param identifier A unique ID for the operation.
   * @param result the result to be handled by a corresponding callback
   * @param <T> a type of event
   */
  public synchronized <T> void onCompleted(final Class<? extends T> eventType,
                                           final String identifier,
                                           final T result) {
    final Map<String, EventHandler> callbackMaps = callbackMapsPerType.get(eventType);
    if (callbackMaps == null) {
      throw new RuntimeException(String.format("Callbacks for event %s are not registered or already completed",
          eventType));
    }

    @SuppressWarnings("unchecked")
    final EventHandler<T> callback = callbackMaps.remove(identifier);
    if (callback == null) {
      throw new RuntimeException(String.format("Callback for event %s and id %s is not registered or already completed",
          eventType, identifier));
    }

    callback.onNext(result);

    if (callbackMaps.isEmpty()) {
      callbackMapsPerType.remove(eventType);
    }
  }
}
