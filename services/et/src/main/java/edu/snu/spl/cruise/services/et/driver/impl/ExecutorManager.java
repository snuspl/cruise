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
package edu.snu.spl.cruise.services.et.driver.impl;

import edu.snu.spl.cruise.services.et.common.util.concurrent.ListenableFuture;
import edu.snu.spl.cruise.services.et.common.util.concurrent.AggregateFuture;
import edu.snu.spl.cruise.services.et.common.impl.CallbackRegistry;
import edu.snu.spl.cruise.services.et.configuration.ExecutorConfiguration;
import edu.snu.spl.cruise.services.et.configuration.ExecutorServiceConfiguration;
import edu.snu.spl.cruise.services.et.configuration.ResourceConfiguration;
import edu.snu.spl.cruise.services.et.configuration.parameters.ETIdentifier;
import edu.snu.spl.cruise.services.et.configuration.parameters.NumTasklets;
import edu.snu.spl.cruise.services.et.configuration.parameters.chkp.ChkpCommitPath;
import edu.snu.spl.cruise.services.et.configuration.parameters.chkp.ChkpTempPath;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.MessageSender;
import edu.snu.spl.cruise.services.et.evaluator.impl.ContextStartHandler;
import edu.snu.spl.cruise.services.et.evaluator.impl.ContextStopHandler;
import edu.snu.spl.cruise.services.et.exceptions.ExecutorNotExistException;
import edu.snu.spl.cruise.services.evalmanager.api.EvaluatorManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.JVMProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.runtime.common.parameters.JVMHeapSlack;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A manager class of executors.
 * It allocates new executors and manages existing executors.
 */
@Private
@DriverSide
final class ExecutorManager {
  private static final Logger LOG = Logger.getLogger(ExecutorManager.class.getName());
  private static final String CONTEXT_PREFIX = "ET-";

  private final CallbackRegistry callbackRegistry;

  private final MessageSender msgSender;
  private final EvaluatorManager evaluatorManager;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;
  private final IdentifierFactory identifierFactory;
  private final String etIdentifier;
  private final String driverIdentifier;
  private final String chkpTempPath;
  private final String chkpCommitPath;

  private final double jvmHeapSlack;
  private final JVMProcessFactory jvmProcessFactory;

  private final AtomicInteger contextIdCounter = new AtomicInteger(0);

  private final Map<String, AllocatedExecutor> executors = new ConcurrentHashMap<>();

  @Inject
  private ExecutorManager(final CallbackRegistry callbackRegistry,
                          final EvaluatorManager evaluatorManager,
                          final MessageSender msgSender,
                          final NameServer nameServer,
                          final LocalAddressProvider localAddressProvider,
                          final IdentifierFactory identifierFactory,
                          final JVMProcessFactory jvmProcessFactory,
                          @Parameter(ChkpCommitPath.class) final String chkpCommitPath,
                          @Parameter(ChkpTempPath.class) final String chkpTempPath,
                          @Parameter(JVMHeapSlack.class) final double jvmHeapSlack,
                          @Parameter(ETIdentifier.class) final String etIdentifier,
                          @Parameter(DriverIdentifier.class) final String driverIdentifier) {
    this.callbackRegistry = callbackRegistry;
    this.evaluatorManager = evaluatorManager;
    this.msgSender = msgSender;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.identifierFactory = identifierFactory;
    this.etIdentifier = etIdentifier;
    this.driverIdentifier = driverIdentifier;
    this.jvmHeapSlack = jvmHeapSlack;
    this.jvmProcessFactory = jvmProcessFactory;
    this.chkpTempPath = chkpTempPath;
    this.chkpCommitPath = chkpCommitPath;
  }

  /**
   * Allocates new {@code num} executors of the equal resource specification.
   * It returns when requested executors are allocated.
   * @param num the number of executors
   * @param executorConf executor configuration
   * @return a list of allocated executors
   */
  ListenableFuture<List<AllocatedExecutor>> addExecutors(final int num, final ExecutorConfiguration executorConf) {
    final int numTasklets = executorConf.getNumTasklets();
    final ResourceConfiguration resConf = executorConf.getResourceConf();
    final Configuration remoteAccessConf = executorConf.getRemoteAccessConf();
    final Configuration userContextConf = executorConf.getUserContextConf();
    final Configuration userServiceConf = executorConf.getUserServiceConf();

    final int numCores = resConf.getNumCores();
    final int memSizeInMB = resConf.getMemSizeInMB();
    final String[] nodeNames = resConf.getNodeNames();

    final ListenableFuture<List<AllocatedExecutor>> executorListFuture = new AggregateFuture<>(num);

    final AtomicInteger executorIdxCounter = new AtomicInteger(0);
    final List<EventHandler<ActiveContext>> activeCtxHandlers = new ArrayList<>(1);
    activeCtxHandlers.add(activeContext -> {
      final AllocatedExecutor allocatedExecutor = new AllocatedExecutorImpl(activeContext, msgSender, callbackRegistry);
      executors.put(allocatedExecutor.getId(), allocatedExecutor);
      LOG.log(Level.INFO, "A new Executor {0} is allocated ({1}/{2}).",
          new Object[]{allocatedExecutor.getId(), executorIdxCounter.incrementAndGet(), num});

      ((AggregateFuture<AllocatedExecutor>) executorListFuture).onCompleted(allocatedExecutor);
    });

    final Configuration serviceConf = Configurations.merge(remoteAccessConf, userServiceConf,
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindNamedParameter(NumTasklets.class, Integer.toString(numTasklets))
            .build());

    evaluatorManager.allocateEvaluators(num, memSizeInMB, numCores, nodeNames,
        new AllocatedEvalHandler(userContextConf, serviceConf, memSizeInMB),
        activeCtxHandlers);

    return executorListFuture;
  }

  /**
   * @return AllocatedExecutor whose id is {@code executorId}
   */
  AllocatedExecutor getExecutor(final String executorId) throws ExecutorNotExistException {
    final AllocatedExecutor executor = executors.get(executorId);
    if (executor == null) {
      throw new ExecutorNotExistException(executorId + " does not exist.");
    }
    return executor;
  }

  /**
   * Removes an executor entry from {@link #executors}.
   * It should be called upon the completion of closing executor.
   * @param executorId an executor id
   */
  void onClosedExecutor(final String executorId) {
    final AllocatedExecutor closedExecutor = executors.remove(executorId);
    if (closedExecutor == null) {
      throw new RuntimeException(String.format("Executor %s does not exist", executorId));
    }

    LOG.log(Level.INFO, "Executor {0} has been closed.", executorId);
    ((AllocatedExecutorImpl) closedExecutor).onFinishClose();
  }

  /**
   * Submits ET context, including user's configuration, which will setup executor when evaluator is allocated.
   */
  private final class AllocatedEvalHandler implements EventHandler<AllocatedEvaluator> {
    private final Configuration contextConf;
    private final Configuration serviceConf;
    private final int memSizeInMB;

    /**
     * @param contextConf a context configuration specified by user
     * @param serviceConf a service configuration specified by user
     */
    AllocatedEvalHandler(final Configuration contextConf,
                         final Configuration serviceConf,
                         final int memSizeInMB) {
      this.contextConf = contextConf;
      this.serviceConf = serviceConf;
      this.memSizeInMB = memSizeInMB;
    }

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration contextConfiguration;

      final Configuration baseContextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, CONTEXT_PREFIX + contextIdCounter.getAndIncrement())
          .set(ContextConfiguration.ON_CONTEXT_STARTED, ContextStartHandler.class)
          .set(ContextConfiguration.ON_CONTEXT_STOP, ContextStopHandler.class)
          .build();

      contextConfiguration = Configurations.merge(baseContextConfiguration, contextConf);

      // different from context configuration, service configuration will be inherited by upper contexts
      final Configuration serviceConfiguration;

      final Configuration executorConfiguration = ExecutorServiceConfiguration.CONF
          .set(ExecutorServiceConfiguration.ET_IDENTIFIER, etIdentifier)
          .set(ExecutorServiceConfiguration.IDENTIFIER, allocatedEvaluator.getId()) // use evaluatorId as executorId
          .set(ExecutorServiceConfiguration.NAME_SERVICE_HOST, localAddressProvider.getLocalAddress())
          .set(ExecutorServiceConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
          .set(ExecutorServiceConfiguration.IDENTIFIER_FACTORY, identifierFactory.getClass())
          .set(ExecutorServiceConfiguration.DRIVER_IDENTIFIER, driverIdentifier)
          .set(ExecutorServiceConfiguration.CHKP_COMMIT_PATH, chkpCommitPath)
          .set(ExecutorServiceConfiguration.CHKP_TEMP_PATH, chkpTempPath)
          .build();

      serviceConfiguration = Configurations.merge(executorConfiguration, serviceConf);

      final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
          .setMemory((int)(memSizeInMB * (1 - jvmHeapSlack)))
          .addOption("-XX:+UseG1GC");

      allocatedEvaluator.setProcess(jvmProcess);

      allocatedEvaluator.submitContextAndService(contextConfiguration, serviceConfiguration);
      LOG.log(Level.FINE, "Submitted context to evaluator {0}", allocatedEvaluator.getId());
    }
  }
}
