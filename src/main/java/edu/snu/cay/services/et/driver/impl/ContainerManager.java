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

import edu.snu.cay.services.et.configuration.ContainerConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedContainer;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@link ContainerManager} implementation.
 * Assumes that REEF allocates evaluators following resource specifications exactly.
 */
@Private
@DriverSide
final class ContainerManager {
  private static final Logger LOG = Logger.getLogger(ContainerManager.class.getName());
  private static final String CONTEXT_PREFIX = "ET-";

  private final EvaluatorManager evaluatorManager;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;
  private final IdentifierFactory identifierFactory;
  private final String driverIdentifier;

  private final AtomicInteger contextIdCounter = new AtomicInteger(0);

  @Inject
  private ContainerManager(final EvaluatorManager evaluatorManager,
                           final NameServer nameServer,
                           final LocalAddressProvider localAddressProvider,
                           final IdentifierFactory identifierFactory,
                           @Parameter(DriverIdentifier.class) final String driverIdentifier) {
    this.evaluatorManager = evaluatorManager;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.identifierFactory = identifierFactory;
    this.driverIdentifier = driverIdentifier;
  }

  /**
   * Allocates new {@code num} containers of the equal resource specification.
   * {@code callback} will be invoked when requested containers are allocated.
   * @param num the number of containers
   * @param resConf resource configuration
   * @param callback callback to invoke when allocating is done
   */
  void addContainers(final int num, final ResourceConfiguration resConf,
                     final EventHandler<AllocatedContainer> callback) {
    final int numCores = resConf.getNumCores();
    final int memSizeInMB = resConf.getMemSizeInMB();

    final List<EventHandler<ActiveContext>> activeCtxHandlers = new ArrayList<>(1);
    activeCtxHandlers.add(new ActiveContextHandler(callback));

    evaluatorManager.allocateEvaluators(num, memSizeInMB, numCores,
        allocatedEvalHandler, activeCtxHandlers);

    LOG.log(Level.INFO, "Requested {0} containers", num);
  }

  /**
   * Submits ET context when evaluator is allocated.
   */
  private final EventHandler<AllocatedEvaluator> allocatedEvalHandler = new EventHandler<AllocatedEvaluator>() {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_PREFIX + contextIdCounter.getAndIncrement())
          .build();
      final Configuration containerConfiguration = ContainerConfiguration.CONF
          .set(ContainerConfiguration.NAME_SERVICE_HOST, localAddressProvider.getLocalAddress())
          .set(ContainerConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
          .set(ContainerConfiguration.IDENTIFIER_FACTORY, identifierFactory.getClass())
          .set(ContainerConfiguration.DRIVER_IDENTIFIER, driverIdentifier)
          .build();
      allocatedEvaluator.submitContext(Configurations.merge(contextConfiguration, containerConfiguration));
      LOG.log(Level.FINE, "Submitted context to evaluator {0}", allocatedEvaluator.getId());
    }
  };

  /**
   * Generates AllocatedContainer and invokes when ET context becomes active.
   */
  private final class ActiveContextHandler implements EventHandler<ActiveContext> {
    private final EventHandler<AllocatedContainer> callback;

    private ActiveContextHandler(final EventHandler<AllocatedContainer> callback) {
      this.callback = callback;
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      final AllocatedContainer allocatedContainer = new AllocatedContainerImpl(activeContext);
      LOG.log(Level.INFO, "Allocated container: {0}", allocatedContainer.getId());
      callback.onNext(allocatedContainer);
    }
  }
}
