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
package edu.snu.cay.services.aggregate;

import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;

/**
 * Register a driver id to a NetworkConnectionService when driver starts.
 */
@Unit
public final class NetworkDriverRegister {

  private final AggregateNetworkSetup aggregateNetworkSetup;
  private final Identifier driverId;

  @Inject
  private NetworkDriverRegister(final AggregateNetworkSetup aggregateNetworkSetup,
                                final IdentifierFactory identifierFactory,
                                @Parameter(DriverIdentifier.class) final String driverIdStr) {
    this.aggregateNetworkSetup = aggregateNetworkSetup;
    this.driverId = identifierFactory.getNewInstance(driverIdStr);
  }

  public final class RegisterDriverHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      aggregateNetworkSetup.registerConnectionFactory(driverId);
    }
  }

  public final class UnregisterDriverHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      aggregateNetworkSetup.unregisterConnectionFactory();
    }
  }
}
