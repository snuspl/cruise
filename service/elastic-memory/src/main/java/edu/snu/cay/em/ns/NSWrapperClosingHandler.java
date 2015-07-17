/**
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

package edu.snu.cay.em.ns;

import edu.snu.cay.em.ns.api.NSWrapper;
import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.io.network.impl.NetworkService;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * EventHandler for closing the NetworkService provided by NSWrapper when
 * context terminates.
 */
public final class NSWrapperClosingHandler implements EventHandler<ContextStop> {

  private final NetworkService networkService;

  @Inject
  private NSWrapperClosingHandler(final NSWrapper nsWrapper) {
    this.networkService = nsWrapper.getNetworkService();
  }

  @Override
  public void onNext(final ContextStop contextStop) {
    try {
      networkService.close();
    } catch (final Exception e) {
      throw new RuntimeException("Exception while closing NetworkService provided by NSWrapper", e);
    }
  }
}
