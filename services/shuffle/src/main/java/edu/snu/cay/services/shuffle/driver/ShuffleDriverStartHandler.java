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
package edu.snu.cay.services.shuffle.driver;

import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.runtime.event.RuntimeStart;

import javax.inject.Inject;

/**
 * RuntimeStartHandler which registers a connection factory for ShuffleControlMessage.
 */
public final class ShuffleDriverStartHandler implements EventHandler<RuntimeStart> {

  private final NetworkConnectionService networkConnectionService;

  // TODO: This will be removed after reef-core includes a NCS configuration module for driver.
  private final Identifier driverId;

  @Inject
  private ShuffleDriverStartHandler(
      @Parameter(NameServerParameters.NameServerIdentifierFactory.class) final IdentifierFactory idFactory,
      final NetworkConnectionService networkConnectionService) {
    this.networkConnectionService = networkConnectionService;
    this.driverId = idFactory.getNewInstance(ShuffleDriverConfiguration.SHUFFLE_DRIVER_NETWORK_IDENTIFIER);
  }

  @Override
  public void onNext(final RuntimeStart runtimeStart) {
    networkConnectionService.registerId(driverId);
  }
}
