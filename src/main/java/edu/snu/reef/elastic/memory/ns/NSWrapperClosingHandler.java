/**
 * Copyright (C) 2014 Seoul National University
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

package edu.snu.reef.elastic.memory.ns;

import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

public final class NSWrapperClosingHandler implements EventHandler<ContextStop> {
  private final NetworkService<?> networkService;

  @Inject
  public NSWrapperClosingHandler(final NSWrapper<?> networkServiceWrapper) {
    this.networkService = networkServiceWrapper.getNetworkService();
  }

  @Override
  public void onNext(ContextStop arg0) {
    try {
      networkService.close();
    } catch (Exception e) {
      throw new RuntimeException("Exception while closing NetworkService", e);
    }
  }

}