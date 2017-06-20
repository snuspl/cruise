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
package edu.snu.cay.dolphin.async.jobserver;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Dolphin on ET.
 * Upon start, it initializes executors and tables for running a dolphin job.
 */
@Unit
public final class JobServerDriver {
  private static final Logger LOG = Logger.getLogger(JobServerDriver.class.getName());

  private final ETMaster etMaster;

  private final String reefJobId;

  private final int numJobsToExecute;
  private final AtomicInteger jobCounter = new AtomicInteger(0);

  /**
   * It maintains {@link DolphinMaster}s of running dolphin jobs.
   */
  private final Map<String, DolphinMaster> dolphinMasterMap = new ConcurrentHashMap<>();

  private final Injector jobBaseInjector;

  // for a single dolphin job
  private final Configuration jobConf;

  private ConfigurationSerializer confSerializer;

  @Inject
  private JobServerDriver(final ETMaster etMaster,
                          final ConfigurationSerializer confSerializer,
                          final NetworkConnection<DolphinMsg> networkConnection,
                          final Injector jobBaseInjector,
                          @Parameter(JobServerLauncher.NumJobs.class) final int numJobsToExecute,
                          @Parameter(DriverIdentifier.class) final String driverId,
                          @Parameter(JobIdentifier.class) final String reefJobId,
                          @Parameter(JobServerLauncher.SerializedJobConf.class) final String serializedJobConf)
      throws IOException, InjectionException {
    this.etMaster = etMaster;

    this.numJobsToExecute = numJobsToExecute;
    this.reefJobId = reefJobId;

    networkConnection.setup(driverId);

    this.jobBaseInjector = jobBaseInjector;

    this.confSerializer = confSerializer;
    this.jobConf = confSerializer.fromString(serializedJobConf);
  }

  /**
   * Gets a {@link DolphinMaster} with {@code dolphinJobId}.
   * @param dolphinJobId a dolphin job identifier
   * @return
   */
  public DolphinMaster getDolphinMaster(final String dolphinJobId) {
    return dolphinMasterMap.get(dolphinJobId);
  }

  private static ResourceConfiguration buildResourceConf(final int numCores, final int memSize) {
    return ResourceConfiguration.newBuilder()
        .setNumCores(numCores)
        .setMemSizeInMB(memSize)
        .build();
  }

  private static RemoteAccessConfiguration buildRemoteAccessConf(final int numSenderThreads,
                                                                 final int senderQueueSize,
                                                                 final int numHandlerThreads,
                                                                 final int handlerQueueSize) {
    return RemoteAccessConfiguration.newBuilder()
        .setNumSenderThreads(numSenderThreads)
        .setSenderQueueSize(senderQueueSize)
        .setNumHandlerThreads(numHandlerThreads)
        .setHandlerQueueSize(handlerQueueSize)
        .build();
  }

  private static TableConfiguration buildWorkerTableConf(final String tableId,
                                                         final Injector workerInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final Codec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final Codec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
    final DataParser dataParser = workerInjector.getInstance(DataParser.class);

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(false)
        .setIsOrderedTable(true)
        .setDataParserClass(dataParser.getClass())
        .setUserParamConf(userParamConf)
        .build();
  }

  private static TableConfiguration buildServerTableConf(final String tableId,
                                                         final Injector serverInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final Codec keyCodec = serverInjector.getNamedInstance(KeyCodec.class);
    final Codec valueCodec = serverInjector.getNamedInstance(ValueCodec.class);
    final Codec updateValueCodec = serverInjector.getNamedInstance(UpdateValueCodec.class);
    final UpdateFunction updateFunction = serverInjector.getInstance(UpdateFunction.class);

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(keyCodec.getClass())
        .setValueCodecClass(valueCodec.getClass())
        .setUpdateValueCodecClass(updateValueCodec.getClass())
        .setUpdateFunctionClass(updateFunction.getClass())
        .setNumTotalBlocks(numTotalBlocks)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(userParamConf)
        .build();
  }

  /**
   * A driver start handler for requesting executors and creating tables to run a dolphin job.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // TODO #1173: execute jobs dynamically
      for (int i = 0; i < numJobsToExecute; i++) {
        try {
          executeJob(jobConf);
        } catch (InjectionException | IOException e) {
          throw new RuntimeException("Exception while running a job", e);
        }
      }
    }
  }

  /**
   * Executes a job with the given configuration.
   * @param jobConfToExecute a job configuration to execute
   */
  private void executeJob(final Configuration jobConfToExecute) throws InjectionException, IOException {
    final Injector jobInjector = jobBaseInjector.forkInjector(jobConfToExecute);

    // generate different dolphin job id for each job
    final int jobCount = jobCounter.getAndIncrement();

    final String dolphinJobId = reefJobId + "-" + jobCount;
    final String modelTableId = ModelTableId.DEFAULT_VALUE + jobCount;
    final String inputTableId = InputTableId.DEFAULT_VALUE + jobCount;

    LOG.log(Level.INFO, "Execute job with Id {0}", dolphinJobId);
    jobInjector.bindVolatileParameter(DolphinJobId.class, dolphinJobId);
    jobInjector.bindVolatileParameter(ModelTableId.class, modelTableId);
    jobInjector.bindVolatileParameter(InputTableId.class, inputTableId);

    final String serializedParamConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedParamConf.class);
    final String serializedServerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedServerConf.class);
    final String serializedWorkerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedWorkerConf.class);

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = confSerializer.fromString(serializedParamConf);

    // prepare server-side configurations
    final Configuration serverConf = confSerializer.fromString(serializedServerConf);
    final Injector serverInjector = Tang.Factory.getTang().newInjector(serverConf);
    final int numServers = serverInjector.getNamedInstance(NumServers.class);
    final int numServerCores = serverInjector.getNamedInstance(NumServerCores.class);
    final int serverMemSize = serverInjector.getNamedInstance(ServerMemSize.class);
    final int numServerSenderThreads = serverInjector.getNamedInstance(NumServerSenderThreads.class);
    final int numServerHandlerThreads = serverInjector.getNamedInstance(NumServerHandlerThreads.class);
    final int serverSenderQueueSize = serverInjector.getNamedInstance(ServerSenderQueueSize.class);
    final int serverHandlerQueueSize = serverInjector.getNamedInstance(ServerHandlerQueueSize.class);
    final int numServerBlocks = serverInjector.getNamedInstance(NumServerBlocks.class);

    final ResourceConfiguration serverResourceConf = buildResourceConf(numServerCores, serverMemSize);
    final RemoteAccessConfiguration serverRemoteAccessConf = buildRemoteAccessConf(
        numServerSenderThreads, serverSenderQueueSize, numServerHandlerThreads, serverHandlerQueueSize);
    final TableConfiguration serverTableConf = buildServerTableConf(modelTableId,
        serverInjector, numServerBlocks, userParamConf);

    // prepare worker-side configurations
    final Configuration workerConf = confSerializer.fromString(serializedWorkerConf);
    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    final int numWorkers = workerInjector.getNamedInstance(NumWorkers.class);
    final int numWorkerCores = workerInjector.getNamedInstance(NumWorkerCores.class);
    final int workerMemSize = workerInjector.getNamedInstance(WorkerMemSize.class);
    final int numWorkerSenderThreads = workerInjector.getNamedInstance(NumWorkerSenderThreads.class);
    final int numWorkerHandlerThreads = workerInjector.getNamedInstance(NumWorkerHandlerThreads.class);
    final int workerSenderQueueSize = workerInjector.getNamedInstance(WorkerSenderQueueSize.class);
    final int workerHandlerQueueSize = workerInjector.getNamedInstance(WorkerHandlerQueueSize.class);
    final int numWorkerBlocks = workerInjector.getNamedInstance(NumWorkerBlocks.class);

    final ResourceConfiguration workerResourceConf = buildResourceConf(numWorkerCores, workerMemSize);
    final RemoteAccessConfiguration workerRemoteAccessConf = buildRemoteAccessConf(
        numWorkerSenderThreads, workerSenderQueueSize, numWorkerHandlerThreads, workerHandlerQueueSize);
    final TableConfiguration workerTableConf = buildWorkerTableConf(inputTableId,
        workerInjector, numWorkerBlocks, userParamConf);
    final String inputPath = workerInjector.getNamedInstance(Parameters.InputDir.class);

    LOG.log(Level.INFO, "Preparing executors and tables for job: {0}", dolphinJobId);
    final Future<List<AllocatedExecutor>> serversFuture = etMaster.addExecutors(numServers,
        ExecutorConfiguration.newBuilder()
            .setResourceConf(serverResourceConf)
            .setRemoteAccessConf(serverRemoteAccessConf)
            .setUserContextConf(NetworkConfProvider.getContextConfiguration())
            .setUserServiceConf(NetworkConfProvider.getServiceConfiguration(reefJobId, dolphinJobId))
            .build());
    final Future<List<AllocatedExecutor>> workersFuture = etMaster.addExecutors(numWorkers,
        ExecutorConfiguration.newBuilder()
            .setResourceConf(workerResourceConf)
            .setRemoteAccessConf(workerRemoteAccessConf)
            .setUserContextConf(NetworkConfProvider.getContextConfiguration())
            .setUserServiceConf(NetworkConfProvider.getServiceConfiguration(reefJobId, dolphinJobId))
            .build());

    new Thread(() -> {
      try {
        final List<AllocatedExecutor> servers = serversFuture.get();
        final List<AllocatedExecutor> workers = workersFuture.get();

        final Future<AllocatedTable> modelTableFuture = etMaster.createTable(serverTableConf, servers);
        final Future<AllocatedTable> inputTableFuture = etMaster.createTable(workerTableConf, workers);

        final AllocatedTable modelTable = modelTableFuture.get();
        final AllocatedTable inputTable = inputTableFuture.get();

        modelTable.subscribe(workers).get();
        inputTable.load(workers, inputPath).get();

        try {
          LOG.log(Level.FINE, "Spawn new dolphinMaster with ID {0}", dolphinJobId);
          final DolphinMaster dolphinMaster = jobInjector.getInstance(DolphinMaster.class);
          dolphinMasterMap.put(dolphinJobId, dolphinMaster);

          dolphinMaster.start(servers, workers, modelTable, inputTable);

          workers.forEach(AllocatedExecutor::close);
          servers.forEach(AllocatedExecutor::close);
        } finally {
          LOG.log(Level.INFO, "Job execution has been finished. JobId: {0}", dolphinJobId);
          dolphinMasterMap.remove(dolphinJobId);
        }

      } catch (InterruptedException | ExecutionException | InjectionException e) {
        LOG.log(Level.INFO, "Exception while running a job");
      }
    }).start();
  }

  /**
   * Handler for FailedContext, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedContext.asError());
    }
  }

  /**
   * Handler for FailedEvaluator, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  /**
   * Handler for FailedTask, which throws RuntimeException to shutdown the entire job.
   */
  final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedTask.asError());
    }
  }
}
