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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedContainer;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;

/**
 * Implementation for {@link ETMaster}.
 */
@DriverSide
public final class ETMasterImpl implements ETMaster {
  private final ContainerManager containerManager;

  @Inject
  private ETMasterImpl(final ContainerManager containerManager,
                       final NetworkConnection networkConnection,
                       final IdentifierFactory idFactory,
                       @Parameter(DriverIdentifier.class) final String driverId) {
    this.containerManager = containerManager;
    networkConnection.setup(idFactory.getNewInstance(driverId));
  }

  @Override
  public void addContainers(final int num, final ResourceConfiguration resConf,
                            final EventHandler<AllocatedContainer> callback)
      throws IllegalArgumentException {
    containerManager.addContainers(num, resConf, callback);
  }
}
