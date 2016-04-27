/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.ps.ns;

import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;

/**
 * Registers the driver id to the NetworkConnectionService when driver starts.
 */
@Unit
public final class NetworkDriverRegister {
  private final PSNetworkSetup psNetworkSetup;
  private final IdentifierFactory identifierFactory;
  private final String driverId;

  @Inject
  private NetworkDriverRegister(final PSNetworkSetup psNetworkSetup,
                                final IdentifierFactory identifierFactory,
                                @Parameter(DriverIdentifier.class) final String driverId) {
    this.psNetworkSetup = psNetworkSetup;
    this.identifierFactory = identifierFactory;
    this.driverId = driverId;
  }

  public final class RegisterDriverHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      psNetworkSetup.registerConnectionFactory(identifierFactory.getNewInstance(driverId));
    }
  }
}
