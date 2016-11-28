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
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedContainer;
import edu.snu.cay.services.et.driver.api.ETMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation for {@link ETMaster}.
 */
@DriverSide
public final class ETMasterImpl implements ETMaster {
  private static final Logger LOG = Logger.getLogger(ETMasterImpl.class.getName());

  private final ContainerManager containerManager;
  private final TableManager tableManager;

  @Inject
  private ETMasterImpl(final ContainerManager containerManager,
                       final TableManager tableManager,
                       final NetworkConnection networkConnection,
                       final IdentifierFactory idFactory,
                       @Parameter(DriverIdentifier.class) final String driverId) {
    this.containerManager = containerManager;
    this.tableManager = tableManager;
    networkConnection.setup(idFactory.getNewInstance(driverId));
  }

  @Override
  public List<AllocatedContainer> addContainers(final int num, final ResourceConfiguration resConf)
      throws IllegalArgumentException {
    final List<AllocatedContainer> containers = Collections.synchronizedList(new ArrayList<>(num));
    final CountDownLatch latch = new CountDownLatch(num);

    containerManager.addContainers(num, resConf, container -> {
      synchronized (containers) {
        containers.add(container);
        LOG.log(Level.INFO, "A new Container is allocated ({0}/{1}).", new Object[]{containers.size(), num});
      }
      latch.countDown();
    });

    // wait until all requested containers are allocated.
    while (true) {
      try {
        latch.await();
        break;
      } catch (final InterruptedException e) {
        LOG.log(Level.WARNING, "Interrupted while waiting for containers to be allocated.", e);
      }
    }

    return containers;
  }

  @Override
  public RawTable createTable(final TableConfiguration tableConf) {
    try {
      return tableManager.createTable(tableConf);
    } catch (final InjectionException e) {
      throw new RuntimeException("The given table configuration is incomplete", e);
    }
  }
}
