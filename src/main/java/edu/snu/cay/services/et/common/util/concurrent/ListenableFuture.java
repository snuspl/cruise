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
package edu.snu.cay.services.et.common.util.concurrent;

import org.apache.reef.wake.EventHandler;

import java.util.concurrent.Future;

/**
 * A future that allows users to register listener through {@link #addListener(EventHandler)}.
 * The registered listeners are invoked when the future is completed.
 */
public interface ListenableFuture<V> extends Future<V> {

  /**
   * Adds a listener, which is in a form of {@link EventHandler}.
   * @param listener a listener
   */
  void addListener(EventHandler<V> listener);
}
