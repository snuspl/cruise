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
import edu.snu.cay.dolphin.async.client.ETDolphinLauncher;
import edu.snu.cay.dolphin.async.core.driver.master.DolphinMaster;
import edu.snu.cay.dolphin.async.network.NetworkConfProvider;
import edu.snu.cay.dolphin.async.network.NetworkConnection;
import edu.snu.cay.dolphin.async.jobserver.Parameters.*;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.ConfigurationUtils;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.runtime.common.driver.parameters.JobIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.dolphin.async.jobserver.Parameters.*;

/**
 * Driver code for Dolphin on ET.
 * It executes a job or finishes itself upon request from clients.
 */
@Unit
public final class JobServerDriver {
  private static final Logger LOG = Logger.getLogger(JobServerDriver.class.getName());

  private final ETMaster etMaster;
  private final JobMessageObserver jobMessageObserver;
  private final JobServerStatusManager jobServerStatusManager;
  private final JobScheduler jobScheduler;

  private final String reefJobId;

  private final AtomicInteger jobCounter = new AtomicInteger(0);

  /**
   * It maintains {@link DolphinMaster}s of running dolphin jobs.
   */
  private final Map<String, DolphinMaster> dolphinMasterMap = new ConcurrentHashMap<>();

  private final Injector jobBaseInjector;

  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  @Inject
  private JobServerDriver(final ETMaster etMaster,
                          final NetworkConnection<DolphinMsg> networkConnection,
                          final JobMessageObserver jobMessageObserver,
                          final JobServerStatusManager jobServerStatusManager,
                          final JobScheduler jobScheduler,
                          final Injector jobBaseInjector,
                          @Parameter(DriverIdentifier.class) final String driverId,
                          @Parameter(JobIdentifier.class) final String reefJobId)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.jobMessageObserver = jobMessageObserver;
    this.jobServerStatusManager = jobServerStatusManager;
    this.jobScheduler = jobScheduler;
    this.reefJobId = reefJobId;
    this.jobBaseInjector = jobBaseInjector;

    networkConnection.setup(driverId);
  }

  /**
   * Gets a {@link DolphinMaster} with {@code dolphinJobId}.
   * @param dolphinJobId a dolphin job identifier
   */
  DolphinMaster getDolphinMaster(final String dolphinJobId) {
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
    final StreamingCodec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
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
    final StreamingCodec keyCodec = serverInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = serverInjector.getNamedInstance(ValueCodec.class);
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
   * A driver start handler for showing network information to client.
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      sendMessageToClient("Now, Job Server is ready to receive commands");
    }
  }

  /**
   * Executes a job.
   */
  public void executeJob(final JobEntity jobEntity) {
    final String jobId = jobEntity.getJobId();

    final String jobStartMsg = String.format("Start executing a job. JobId: %s", jobId);
    sendMessageToClient(jobStartMsg);
    LOG.log(Level.INFO, jobStartMsg);

    LOG.log(Level.INFO, "Preparing executors and tables for job: {0}", jobId);
    final Future<List<AllocatedExecutor>> serversFuture = etMaster.addExecutors(jobEntity.getNumServers(),
        jobEntity.getServerExecutorConf());
    final Future<List<AllocatedExecutor>> workersFuture = etMaster.addExecutors(jobEntity.getNumWorkers(),
        jobEntity.getWorkerExecutorConf());

    new Thread(() -> {
      try {
        final List<AllocatedExecutor> servers = serversFuture.get();
        final List<AllocatedExecutor> workers = workersFuture.get();

        final Future<AllocatedTable> modelTableFuture = etMaster.createTable(jobEntity.getServerTableConf(), servers);
        final Future<AllocatedTable> inputTableFuture = etMaster.createTable(jobEntity.getWorkerTableConf(), workers);

        final AllocatedTable modelTable = modelTableFuture.get();
        final AllocatedTable inputTable = inputTableFuture.get();

        modelTable.subscribe(workers).get();
        inputTable.load(workers, jobEntity.getInputPath()).get();

        try {
          LOG.log(Level.FINE, "Spawn new dolphinMaster with ID: {0}", jobId);
          final DolphinMaster dolphinMaster = jobEntity.getJobInjector().getInstance(DolphinMaster.class);
          dolphinMasterMap.put(jobId, dolphinMaster);

          dolphinMaster.start(servers, workers, modelTable, inputTable);

          workers.forEach(AllocatedExecutor::close);
          servers.forEach(AllocatedExecutor::close);

        } finally {
          final String jobFinishMsg = String.format("Job execution has been finished. JobId: %s", jobId);
          LOG.log(Level.INFO, jobFinishMsg);
          sendMessageToClient(jobFinishMsg);
          dolphinMasterMap.remove(jobId);
          jobScheduler.onJobFinish(workers.size() + servers.size());
        }

      } catch (InterruptedException | ExecutionException | InjectionException e) {
        LOG.log(Level.SEVERE, "Exception while running a job");
        throw new RuntimeException(e);
      }
    }).start();
  }

  /**
   * Build a JobEntity from a given job configuration.
   * @param jobConf a job configuration
   * @throws InjectionException
   * @throws IOException
   */
  private JobEntity getJobEntity(final Configuration jobConf) throws InjectionException, IOException {
    final Injector jobInjector = jobBaseInjector.forkInjector(jobConf);

    // generate different dolphin job id for each job
    final int jobCount = jobCounter.getAndIncrement();

    final String appId = jobInjector.getNamedInstance(AppIdentifier.class);
    final String dolphinJobId = appId + "-" + jobCount;
    final String modelTableId = ModelTableId.DEFAULT_VALUE + jobCount;
    final String inputTableId = InputTableId.DEFAULT_VALUE + jobCount;

    jobInjector.bindVolatileParameter(DolphinJobId.class, dolphinJobId);
    jobInjector.bindVolatileParameter(ModelTableId.class, modelTableId);
    jobInjector.bindVolatileParameter(InputTableId.class, inputTableId);

    final String serializedParamConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedParamConf.class);
    final String serializedServerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedServerConf.class);
    final String serializedWorkerConf = jobInjector.getNamedInstance(ETDolphinLauncher.SerializedWorkerConf.class);

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = ConfigurationUtils.fromString(serializedParamConf);

    // prepare server-side configurations
    final Configuration serverConf = ConfigurationUtils.fromString(serializedServerConf);
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

    final ExecutorConfiguration serverExecutorConf = ExecutorConfiguration.newBuilder()
            .setResourceConf(serverResourceConf)
            .setRemoteAccessConf(serverRemoteAccessConf)
            .setUserContextConf(NetworkConfProvider.getContextConfiguration())
            .setUserServiceConf(NetworkConfProvider.getServiceConfiguration(reefJobId, dolphinJobId))
            .build();
    final TableConfiguration serverTableConf = buildServerTableConf(modelTableId,
        serverInjector, numServerBlocks, userParamConf);

    // prepare worker-side configurations
    final Configuration workerConf = ConfigurationUtils.fromString(serializedWorkerConf);
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

    final ExecutorConfiguration workerExecutorConf = ExecutorConfiguration.newBuilder()
            .setResourceConf(workerResourceConf)
            .setRemoteAccessConf(workerRemoteAccessConf)
            .setUserContextConf(NetworkConfProvider.getContextConfiguration())
            .setUserServiceConf(NetworkConfProvider.getServiceConfiguration(reefJobId, dolphinJobId))
            .build();
    final TableConfiguration workerTableConf = buildWorkerTableConf(inputTableId,
        workerInjector, numWorkerBlocks, userParamConf);
    final String inputPath = workerInjector.getNamedInstance(Parameters.InputDir.class);

    return JobEntity.newBuilder()
        .setJobInjector(jobInjector)
        .setJobId(dolphinJobId)
        .setNumServers(numServers)
        .setServerExecutorConf(serverExecutorConf)
        .setServerTableConf(serverTableConf)
        .setNumWorkers(numWorkers)
        .setWorkerExecutorConf(workerExecutorConf)
        .setWorkerTableConf(workerTableConf)
        .setInputPath(inputPath)
        .build();
  }

  /**
   * Initiates a shutdown in which previously submitted jobs are executed, but no new jobs will be accepted.
   * Invocation has no additional effect if already shut down.
   */
  private void shutdown() {
    final String shutdownMsg = "Initiates shutdown of JobServer";

    sendMessageToClient(shutdownMsg);
    LOG.log(Level.INFO, shutdownMsg);
    if (isClosed.compareAndSet(false, true)) {
      jobServerStatusManager.finishJobServer();
    }
  }

  /**
   * Handles command message from client.
   * There are following commands:
   *    SUBMIT                    to submit a new job.
   *    SHUTDOWN                  to shutdown the job server.
   */
  final class ClientMessageHandler implements EventHandler<byte[]> {

    @Override
    public synchronized void onNext(final byte[] bytes) {
      final String input = new String(bytes);
      final String[] result = input.split(COMMAND_DELIMITER, 2);
      final String command = result[0];

      // ignore commands
      if (isClosed.get()) {
        final String rejectMsg = String.format("Job Server is being shut down. Rejected command: %s", command);
        LOG.log(Level.INFO, rejectMsg);
        sendMessageToClient(rejectMsg);
        return;
      }

      switch (command) {
      case SUBMIT_COMMAND:
        try {
          final String serializedConf = result[1];
          final Configuration jobConf = ConfigurationUtils.fromString(serializedConf);
          final JobEntity jobEntity = getJobEntity(jobConf);

          final boolean isAccepted = jobScheduler.onJobArrival(jobEntity);

          final String jobAcceptMsg = isAccepted ?
              String.format("Accept. JobId: %s", jobEntity.getJobId()) :
              String.format("Reject. JobId: %s", jobEntity.getJobId());
          sendMessageToClient(jobAcceptMsg);
          LOG.log(Level.INFO, jobAcceptMsg);

        } catch (InjectionException | IOException e) {
          throw new RuntimeException("The given job configuration is incomplete", e);
        }
        break;
      case SHUTDOWN_COMMAND:
        shutdown();
        break;
      default:
        throw new RuntimeException("There is unexpected command");
      }
    }
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

  private void sendMessageToClient(final String message) {
    jobMessageObserver.sendMessageToClient(message.getBytes());
  }
}
