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
package edu.snu.cay.services.ps.examples.add;

import edu.snu.cay.services.ps.ParameterServerConfigurationBuilder;
import edu.snu.cay.services.ps.driver.impl.PartitionedParameterServerManager;
import edu.snu.cay.services.ps.examples.add.parameters.*;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerNumPartitions;
import edu.snu.cay.services.ps.server.partitioned.parameters.ServerQueueSize;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerExpireTimeout;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerKeyCacheSize;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerNumPartitions;
import edu.snu.cay.services.ps.worker.partitioned.parameters.WorkerQueueSize;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ParameterServer Example for the Partitioned (single-node) PS.
 */
public final class PartitionedPSExampleREEF {
  private static final Logger LOG = Logger.getLogger(PartitionedPSExampleREEF.class.getName());

  private final long timeout;
  private final int numWorkers;
  private final int numUpdates;
  private final int numKeys;
  private final int startKey;
  private final int serverNumPartitions;
  private final int serverQueueSize;
  private final int workerNumPartitions;
  private final int workerQueueSize;
  private final long workerExpireTimeout;
  private final int workerKeyCacheSize;

  @Inject
  private PartitionedPSExampleREEF(@Parameter(JobTimeout.class) final long timeout,
                                   @Parameter(NumWorkers.class) final int numWorkers,
                                   @Parameter(NumUpdates.class) final int numUpdates,
                                   @Parameter(NumKeys.class) final int numKeys,
                                   @Parameter(StartKey.class) final int startKey,
                                   @Parameter(ServerNumPartitions.class) final int serverNumPartitions,
                                   @Parameter(ServerQueueSize.class) final int serverQueueSize,
                                   @Parameter(WorkerNumPartitions.class) final int workerNumPartitions,
                                   @Parameter(WorkerQueueSize.class) final int workerQueueSize,
                                   @Parameter(WorkerExpireTimeout.class) final long workerExpireTimeout,
                                   @Parameter(WorkerKeyCacheSize.class) final int workerKeyCacheSize) {
    this.timeout = timeout;
    this.numWorkers = numWorkers;
    this.numUpdates = numUpdates;
    this.numKeys = numKeys;
    this.startKey = startKey;
    this.serverNumPartitions = serverNumPartitions;
    this.serverQueueSize = serverQueueSize;
    this.workerNumPartitions = workerNumPartitions;
    this.workerQueueSize = workerQueueSize;
    this.workerExpireTimeout = workerExpireTimeout;
    this.workerKeyCacheSize = workerKeyCacheSize;
  }

  private Configuration getDriverConf() {
    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            EnvironmentUtils.getClassLocation(PSExampleDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "PartitionedPSExample")
        .set(DriverConfiguration.ON_DRIVER_STARTED,
            PSExampleDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED,
            PSExampleDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE,
            PSExampleDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING,
            PSExampleDriver.RunningTaskHandler.class)
        .set(DriverConfiguration.ON_TASK_COMPLETED,
            PSExampleDriver.CompletedTaskHandler.class)
        .build();

    final Configuration parametersConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumWorkers.class, Integer.toString(numWorkers))
        .bindNamedParameter(NumUpdates.class, Integer.toString(numUpdates))
        .bindNamedParameter(NumKeys.class, Integer.toString(numKeys))
        .bindNamedParameter(StartKey.class, Integer.toString(startKey))
        .bindNamedParameter(ServerNumPartitions.class, Integer.toString(serverNumPartitions))
        .bindNamedParameter(ServerQueueSize.class, Integer.toString(serverQueueSize))
        .bindNamedParameter(WorkerNumPartitions.class, Integer.toString(workerNumPartitions))
        .bindNamedParameter(WorkerQueueSize.class, Integer.toString(workerQueueSize))
        .bindNamedParameter(WorkerExpireTimeout.class, Long.toString(workerExpireTimeout))
        .bindNamedParameter(WorkerKeyCacheSize.class, Integer.toString(workerKeyCacheSize))
        .build();

    final Configuration psConf = new ParameterServerConfigurationBuilder()
        .setManagerClass(PartitionedParameterServerManager.class)
        .setUpdaterClass(AddUpdater.class)
        .setKeyCodecClass(IntegerCodec.class)
        .setPreValueCodecClass(IntegerCodec.class)
        .setValueCodecClass(IntegerCodec.class)
        .build();

    return Configurations.merge(driverConf, parametersConf, psConf);
  }

  private Configuration getRuntimeConfiguration() {
    return getLocalRuntimeConfiguration();
  }

  private Configuration getLocalRuntimeConfiguration() {
    return LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, numWorkers + 1)
        .build();
  }

  private LauncherStatus run() throws InjectionException {
    return DriverLauncher.getLauncher(getRuntimeConfiguration()).run(getDriverConf(), timeout);
  }

  private static PartitionedPSExampleREEF parseCommandLine(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(JobTimeout.class);
    cl.registerShortNameOfClass(NumWorkers.class);
    cl.registerShortNameOfClass(NumUpdates.class);
    cl.registerShortNameOfClass(NumKeys.class);
    cl.registerShortNameOfClass(StartKey.class);
    cl.registerShortNameOfClass(ServerNumPartitions.class);
    cl.registerShortNameOfClass(ServerQueueSize.class);
    cl.registerShortNameOfClass(WorkerNumPartitions.class);
    cl.registerShortNameOfClass(WorkerQueueSize.class);
    cl.registerShortNameOfClass(WorkerExpireTimeout.class);
    cl.registerShortNameOfClass(WorkerKeyCacheSize.class);

    cl.processCommandLine(args);

    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(PartitionedPSExampleREEF.class);
  }

  public static void main(final String[] args) {
    LauncherStatus status;
    try {
      final PartitionedPSExampleREEF singleNodePSExampleREEF = parseCommandLine(args);
      status = singleNodePSExampleREEF.run();
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "Fatal exception occurred: {0}", e);
      status = LauncherStatus.failed(e);
    }
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
