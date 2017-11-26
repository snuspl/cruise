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
import edu.snu.cay.pregel.common.DefaultVertexCodec;
import edu.snu.cay.pregel.common.MessageCodec;
import edu.snu.cay.pregel.PregelParameters.*;
import edu.snu.cay.pregel.common.MessageUpdateFunction;
import edu.snu.cay.services.et.configuration.ExecutorConfiguration;
import edu.snu.cay.services.et.configuration.RemoteAccessConfiguration;
import edu.snu.cay.services.et.configuration.ResourceConfiguration;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.ETMaster;
import edu.snu.cay.services.et.driver.api.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.RunningTasklet;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.cay.utils.ConfigurationUtils;
import edu.snu.cay.utils.NullCodec;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
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
  private final AtomicInteger workerCounter = new AtomicInteger(0);
  private final Injector masterConfInjector;
  private final Configuration taskConf;

  @Inject
  private PregelDriver(final ETMaster etMaster,
                       final CentCommConfProvider centCommConfProvider,
                       final PregelMaster pregelMaster,
                       @Parameter(SerializedTaskConf.class) final String serializedTaskConf,
                       @Parameter(SerializedMasterConf.class) final String serializedMasterConf)
      throws IOException {
    this.etMaster = etMaster;
    this.masterConfInjector = Tang.Factory.getTang().newInjector(ConfigurationUtils.fromString(serializedMasterConf));
    this.taskConf = ConfigurationUtils.fromString(serializedTaskConf);
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

      new Thread(() -> {
        try {
          etMaster.createTable(buildMsgTableConf(MSG_TABLE_1_ID), executors).get();
          etMaster.createTable(buildMsgTableConf(MSG_TABLE_2_ID), executors).get();
          final AllocatedTable vertexTable = etMaster.createTable(
              buildVertexTableConf(VERTEX_TABLE_ID), executors).get();

          vertexTable.load(executors, masterConfInjector.getNamedInstance(InputPath.class)).get();

          final List<Future<RunningTasklet>> taskFutureList = new ArrayList<>();
          executors.forEach(executor -> taskFutureList.add(executor.submitTask(buildTaskConf())));

          for (final Future<RunningTasklet> submittedTaskFuture : taskFutureList) {
            submittedTaskFuture.get().getTaskResult();
          }
          executors.forEach(AllocatedExecutor::close);

        } catch (InterruptedException | ExecutionException | InjectionException e) {
          throw new RuntimeException(e);
        }
      }).start();
    }
  }

  private Configuration buildTaskConf() {
    return Configurations.merge(taskConf, TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, WORKER_PREFIX + workerCounter.getAndIncrement())
        .set(TaskConfiguration.TASK, PregelWorkerTask.class)
        .build());
  }

  /**
   * Build a configuration of vertex table.
   * Need to provide codecs for vertex value and edge.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildVertexTableConf(final String tableId) throws InjectionException {

    final DataParser dataParser = masterConfInjector.getInstance(DataParser.class);

    // configure vertex value codec, edge codec to vertex table
    final StreamingCodec vertexValueCodec = masterConfInjector.getNamedInstance(VertexValueCodec.class);
    final StreamingCodec edgeCodec = masterConfInjector.getNamedInstance(EdgeCodec.class);
    final Configuration vertexComponentCodecConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(VertexValueCodec.class, vertexValueCodec.getClass())
        .bindNamedParameter(EdgeCodec.class, edgeCodec.getClass())
        .build();

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(DefaultVertexCodec.class) // TODO #1223: allow other types of vertex implementation
        .setUpdateValueCodecClass(NullCodec.class)
        .setUpdateFunctionClass(VoidUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setDataParserClass(dataParser.getClass())
        .setBulkDataLoaderClass(ExistKeyBulkDataLoader.class)
        .setUserParamConf(vertexComponentCodecConf)
        .build();
  }

  /**
   * Build a configuration of message table.
   * Type of value is {@link Iterable} so set {@link MessageCodec} to value codec class.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildMsgTableConf(final String tableId) throws InjectionException {

    // configure message value codec to message table
    final StreamingCodec messageValueStreamingCodec = masterConfInjector.getNamedInstance(MessageValueCodec.class);
    final Configuration messageValueConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageValueCodec.class, messageValueStreamingCodec.getClass())
        .build();

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(MessageCodec.class)
        .setUpdateValueCodecClass(((Codec) messageValueStreamingCodec).getClass())
        .setUpdateFunctionClass(MessageUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(messageValueConf)
        .build();
  }
}
