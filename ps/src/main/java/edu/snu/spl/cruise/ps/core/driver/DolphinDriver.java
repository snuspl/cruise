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
package edu.snu.spl.cruise.ps.core.driver;

import edu.snu.spl.cruise.common.param.Parameters;
import edu.snu.spl.cruise.ps.core.master.DolphinMaster;
import edu.snu.spl.cruise.ps.DolphinParameters.*;
import edu.snu.spl.cruise.ps.core.client.ETDolphinLauncher;
import edu.snu.spl.cruise.services.et.configuration.ExecutorConfiguration;
import edu.snu.spl.cruise.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.spl.cruise.services.et.configuration.ResourceConfiguration;
import edu.snu.spl.cruise.services.et.configuration.TableConfiguration;
import edu.snu.spl.cruise.services.et.configuration.parameters.KeyCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.ValueCodec;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedTable;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import edu.snu.spl.cruise.services.et.evaluator.api.UpdateFunction;
import edu.snu.spl.cruise.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
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
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Dolphin on ET.
 * Upon start, it initializes executors and tables for running a dolphin job.
 */
@Unit
public final class DolphinDriver {
  private static final Logger LOG = Logger.getLogger(DolphinDriver.class.getName());

  private final ETMaster etMaster;
  private final DolphinMaster dolphinMaster;
  private final JobMessageObserver jobMessageObserver;

  private final boolean offlineModelEval;

  private final int numWorkers;
  private final int numServers;

  private final ResourceConfiguration workerResourceConf;
  private final ResourceConfiguration serverResourceConf;

  private final RemoteAccessConfiguration workerRemoteAccessConf;
  private final RemoteAccessConfiguration serverRemoteAccessConf;

  private final TableConfiguration workerTableConf;
  private final TableConfiguration serverTableConf;
  private final String inputPath;

  private final String jobId;

  @Inject
  private DolphinDriver(final DolphinMaster dolphinMaster,
                        final ETMaster etMaster,
                        final JobMessageObserver jobMessageObserver,
                        final ConfigurationSerializer confSerializer,
                        @Parameter(OfflineModelEvaluation.class) final boolean offlineModelEval,
                        @Parameter(NumServers.class) final int numServers,
                        @Parameter(ServerMemSize.class) final int serverMemSize,
                        @Parameter(NumServerCores.class) final int numServerCores,
                        @Parameter(NumWorkers.class) final int numWorkers,
                        @Parameter(WorkerMemSize.class) final int workerMemSize,
                        @Parameter(NumWorkerCores.class) final int numWorkerCores,
                        @Parameter(NumServerSenderThreads.class) final int numServerSenderThreads,
                        @Parameter(NumServerHandlerThreads.class) final int numServerHandlerThreads,
                        @Parameter(ServerSenderQueueSize.class) final int serverSenderQueueSize,
                        @Parameter(ServerHandlerQueueSize.class) final int serverHandlerQueueSize,
                        @Parameter(NumWorkerSenderThreads.class) final int numWorkerSenderThreads,
                        @Parameter(NumWorkerHandlerThreads.class) final int numWorkerHandlerThreads,
                        @Parameter(WorkerSenderQueueSize.class) final int workerSenderQueueSize,
                        @Parameter(WorkerHandlerQueueSize.class) final int workerHandlerQueueSize,
                        @Parameter(NumWorkerBlocks.class) final int numWorkerBlocks,
                        @Parameter(NumServerBlocks.class) final int numServerBlocks,
                        @Parameter(JobIdentifier.class) final String jobId,
                        @Parameter(ETDolphinLauncher.SerializedParamConf.class) final String serializedParamConf,
                        @Parameter(ETDolphinLauncher.SerializedWorkerConf.class) final String serializedWorkerConf,
                        @Parameter(ETDolphinLauncher.SerializedServerConf.class) final String serializedServerConf)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.dolphinMaster = dolphinMaster;
    this.jobMessageObserver = jobMessageObserver;

    this.offlineModelEval = offlineModelEval;

    this.numWorkers = numWorkers;
    this.numServers = numServers;

    // configuration commonly used in both workers and servers
    final Configuration userParamConf = confSerializer.fromString(serializedParamConf);

    // initialize server-side configurations
    final Configuration serverConf = confSerializer.fromString(serializedServerConf);
    final Injector serverInjector = Tang.Factory.getTang().newInjector(serverConf);
    this.serverResourceConf = buildResourceConf(numServerCores, serverMemSize);
    this.serverRemoteAccessConf = buildRemoteAccessConf(numServerSenderThreads, serverSenderQueueSize,
        numServerHandlerThreads, serverHandlerQueueSize);
    this.serverTableConf = buildServerTableConf(serverInjector, numServerBlocks, userParamConf);

    // initialize worker-side configurations
    final Configuration workerConf = confSerializer.fromString(serializedWorkerConf);
    final Injector workerInjector = Tang.Factory.getTang().newInjector(workerConf);
    this.workerResourceConf = buildResourceConf(numWorkerCores, workerMemSize);
    this.workerRemoteAccessConf = buildRemoteAccessConf(numWorkerSenderThreads, workerSenderQueueSize,
        numWorkerHandlerThreads, workerHandlerQueueSize);
    this.workerTableConf = buildWorkerTableConf(workerInjector, numWorkerBlocks, userParamConf);
    this.inputPath = workerInjector.getNamedInstance(Parameters.InputDir.class);
    this.jobId = jobId;
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

  private static TableConfiguration buildWorkerTableConf(final Injector workerInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final StreamingCodec keyCodec = workerInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = workerInjector.getNamedInstance(ValueCodec.class);
    final DataParser dataParser = workerInjector.getInstance(DataParser.class);

    return TableConfiguration.newBuilder()
        .setId(InputTableId.DEFAULT_VALUE)
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

  private static TableConfiguration buildServerTableConf(final Injector serverInjector,
                                                         final int numTotalBlocks,
                                                         final Configuration userParamConf) throws InjectionException {
    final StreamingCodec keyCodec = serverInjector.getNamedInstance(KeyCodec.class);
    final StreamingCodec valueCodec = serverInjector.getNamedInstance(ValueCodec.class);
    final Codec updateValueCodec = serverInjector.getNamedInstance(UpdateValueCodec.class);
    final UpdateFunction updateFunction = serverInjector.getInstance(UpdateFunction.class);

    return TableConfiguration.newBuilder()
        .setId(ModelTableId.DEFAULT_VALUE)
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

  public ExecutorConfiguration getWorkerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(workerResourceConf)
        .setRemoteAccessConf(workerRemoteAccessConf)
        .build();
  }

  public ExecutorConfiguration getServerExecutorConf() {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(serverResourceConf)
        .setRemoteAccessConf(serverRemoteAccessConf)
        .build();
  }

  /**
   * A driver start handler for requesting executors and creating tables to run a dolphin job.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try {
        final List<AllocatedExecutor> servers = etMaster.addExecutors(numServers, getServerExecutorConf()).get();
        final List<AllocatedExecutor> workers = etMaster.addExecutors(numWorkers, getWorkerExecutorConf()).get();

        new Thread(() -> {
          final Future<AllocatedTable> modelTableFuture = etMaster.createTable(serverTableConf, servers);
          final Future<AllocatedTable> inputTableFuture = etMaster.createTable(workerTableConf, workers);

          try {
            final AllocatedTable modelTable = modelTableFuture.get();
            final AllocatedTable inputTable = inputTableFuture.get();

            modelTable.subscribe(workers).get();
            inputTable.load(workers, inputPath).get();

            jobMessageObserver.sendMessageToClient("Start training a model".getBytes());
            dolphinMaster.start(servers, workers, modelTable, inputTable);

            // need to evaluate model tables loaded from checkpoints with new workers executors
            if (offlineModelEval) {
              jobMessageObserver.sendMessageToClient("Start evaluating the trained model".getBytes());

              // TODO #1248: need a support that flushes out all remaining operations when dropping the table
              // sleep before dropping table for preventing loss of ongoing non-blocking ops(pushes)
              Thread.sleep(30000);

              inputTable.drop().get();
              workers.forEach(worker -> {
                try {
                  modelTable.unsubscribe(worker.getId()).get();
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });

              workers.forEach(AllocatedExecutor::close);

              // start model evaluation with new workers (to completely empty the memory)
              final List<AllocatedExecutor> workersForEvaluation
                  = etMaster.addExecutors(workers.size(), getWorkerExecutorConf()).get();
              final AllocatedTable dummyTable = etMaster.createTable(
                  TableConfiguration.newBuilder()
                      .setId(InputTableId.DEFAULT_VALUE)
                      .setKeyCodecClass(StreamingSerializableCodec.class)
                      .setValueCodecClass(StreamingSerializableCodec.class)
                      .setUpdateValueCodecClass(SerializableCodec.class)
                      .setUpdateFunctionClass(VoidUpdateFunction.class)
                      .setIsMutableTable(false)
                      .setIsOrderedTable(true)
                      .build(),
                  workersForEvaluation).get();

              modelTable.subscribe(workersForEvaluation).get();

              dolphinMaster.evaluate(servers, workersForEvaluation);

              workersForEvaluation.forEach(AllocatedExecutor::close);
              servers.forEach(AllocatedExecutor::close);

            } else {
              workers.forEach(AllocatedExecutor::close);
              servers.forEach(AllocatedExecutor::close);
            }
          } catch (Exception e) {
            LOG.log(Level.SEVERE, "Exception while running a job", e);
            throw new RuntimeException(e);
          }
        }).start();

      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Handler for FailedContext, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedContext.asError());
    }
  }

  /**
   * Handler for FailedEvaluator, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
    }
  }

  /**
   * Handler for FailedTask, which throws RuntimeException to shutdown the entire job.
   */
  public final class FailedTaskHandler implements EventHandler<FailedTask> {
    @Override
    public void onNext(final FailedTask failedTask) {
      // TODO #677: Handle failure from Evaluators properly
      throw new RuntimeException(failedTask.asError());
    }
  }
}
