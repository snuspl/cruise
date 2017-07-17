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
package edu.snu.cay.pregel;

import edu.snu.cay.common.centcomm.master.CentCommConfProvider;
import edu.snu.cay.pregel.common.AddDoubleUpdateFunction;
import edu.snu.cay.pregel.common.DoubleMsgCodec;
import edu.snu.cay.pregel.common.NoneEdgeValueGraphParser;
import edu.snu.cay.pregel.common.VertexCodec;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Driver code for Pregel applications.
 */
@Unit
public final class PregelDriver {
  private static final String WORKER_PREFIX = "Worker-";

  public static final String VERTEX_TABLE_ID = "Vertex_table";
  public static final String MSG_TABLE_1_ID = "Msg_table_1";
  public static final String MSG_TABLE_2_ID = "Msg_table_2";

  static final String CENTCOMM_CLIENT_ID = "CENTCOMM_CLIENT_ID";

  // TODO #1178: expose more commandline options
  static final int NUM_EXECUTORS = 3;

  private final ETMaster etMaster;
  private final ExecutorConfiguration executorConf;
  private final String tableInputPath;
  private final AtomicInteger workerCounter = new AtomicInteger(0);

  @Inject
  private PregelDriver(final ETMaster etMaster,
                       final CentCommConfProvider centCommConfProvider,
                       final PregelMaster pregelMaster,
                       @Parameter(PregelLauncher.InputPath.class) final String tableInputPath) {
    this.etMaster = etMaster;
    this.tableInputPath = tableInputPath;
    this.executorConf = ExecutorConfiguration.newBuilder()
        .setResourceConf(ResourceConfiguration.newBuilder()
            .setNumCores(1)
            .setMemSizeInMB(128)
            .build())
        .setRemoteAccessConf(RemoteAccessConfiguration.newBuilder()
            .setHandlerQueueSize(2048)
            .setNumHandlerThreads(1)
            .setSenderQueueSize(2048)
            .setNumSenderThreads(1)
            .build())
        .setUserContextConf(centCommConfProvider.getContextConfiguration())
        .setUserServiceConf(centCommConfProvider.getServiceConfWithoutNameResolver())
        .build();
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {

      final List<AllocatedExecutor> executors;

      try {
        executors = etMaster.addExecutors(NUM_EXECUTORS, executorConf).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      CatchableExecutors.newSingleThreadExecutor().submit(() -> {
        try {
          etMaster.createTable(buildMsgTableConf(MSG_TABLE_1_ID), executors).get();
          etMaster.createTable(buildMsgTableConf(MSG_TABLE_2_ID), executors).get();
          final AllocatedTable vertexTable = etMaster.createTable(
              buildVertexTableConf(VERTEX_TABLE_ID), executors).get();

          vertexTable.load(executors, tableInputPath).get();

          final List<Future<SubmittedTask>> taskFutureList = new ArrayList<>();
          executors.forEach(executor -> taskFutureList.add(executor.submitTask(buildTaskConf())));

          for (final Future<SubmittedTask> submittedTaskFuture : taskFutureList) {
            submittedTaskFuture.get().getTaskResult();
          }
          executors.forEach(AllocatedExecutor::close);

        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private Configuration buildTaskConf() {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, WORKER_PREFIX + workerCounter.getAndIncrement())
        .set(TaskConfiguration.TASK, PregelWorkerTask.class)
        .build();
  }

  /**
   * Build a configuration of vertex table.
   * Type of value is {@link edu.snu.cay.pregel.graph.api.Vertex} so set {@link VertexCodec} to value codec class.
   * Note that this configuration is for Pagerank app.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildVertexTableConf(final String tableId) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(VertexCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setDataParserClass(NoneEdgeValueGraphParser.class)
        .setBulkDataLoaderClass(ExistKeyBulkDataLoader.class)
        .build();
  }

  /**
   * Build a configuration of message table.
   * Type of value is {@link Iterable<Double>} so set {@link DoubleMsgCodec} to value codec class.
   * Note that this configuration is for Pagerank app.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildMsgTableConf(final String tableId) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(DoubleMsgCodec.class)
        .setUpdateValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(AddDoubleUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .build();
  }
}
