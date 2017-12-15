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
package edu.snu.spl.cruise.pregel;

import edu.snu.spl.cruise.common.centcomm.master.CentCommConfProvider;
import edu.snu.spl.cruise.common.param.Parameters;
import edu.snu.spl.cruise.pregel.common.DefaultVertexCodec;
import edu.snu.spl.cruise.pregel.common.MessageCodec;
import edu.snu.spl.cruise.pregel.PregelParameters.*;
import edu.snu.spl.cruise.pregel.common.MessageUpdateFunction;
import edu.snu.spl.cruise.services.et.configuration.*;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedExecutor;
import edu.snu.spl.cruise.services.et.driver.api.ETMaster;
import edu.snu.spl.cruise.services.et.driver.api.AllocatedTable;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import edu.snu.spl.cruise.services.et.evaluator.impl.ExistKeyBulkDataLoader;
import edu.snu.spl.cruise.services.et.evaluator.impl.VoidUpdateFunction;
import edu.snu.spl.cruise.utils.NullCodec;
import edu.snu.spl.cruise.utils.StreamingSerializableCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Driver code for Pregel applications.
 */
@Unit
public final class PregelDriver {
  public static final String VERTEX_TABLE_ID = "Vertex_table";
  public static final String MSG_TABLE_1_ID = "Msg_table_1";
  public static final String MSG_TABLE_2_ID = "Msg_table_2";

  static final String CENTCOMM_CLIENT_ID = "CENTCOMM_CLIENT_ID";

  private final ETMaster etMaster;
  private final int numWorkers;
  private final String inputDir;
  private final ExecutorConfiguration executorConf;

  private final PregelMaster pregelMaster;

  private final TableConfiguration vertexTableConf;
  private final TableConfiguration msgTable1Conf;
  private final TableConfiguration msgTable2Conf;

  @Inject
  private PregelDriver(final ETMaster etMaster,
                       @Parameter(NumWorkers.class) final int numWorkers,
                       @Parameter(WorkerMemSize.class) final int workerMemSize,
                       @Parameter(WorkerNumCores.class) final int workerNumCores,
                       @Parameter(Parameters.InputDir.class) final String inputDir,
                       final CentCommConfProvider centCommConfProvider,
                       final PregelMaster pregelMaster,
                       final DataParser dataParser,
                       @Parameter(VertexValueCodec.class) final StreamingCodec vertexValueCodec,
                       @Parameter(EdgeCodec.class) final StreamingCodec edgeCodec,
                       @Parameter(MessageValueCodec.class) final StreamingCodec msgValueCodec)
      throws IOException, InjectionException {
    this.etMaster = etMaster;
    this.numWorkers = numWorkers;
    this.inputDir = inputDir;
    this.pregelMaster = pregelMaster;
    this.executorConf = buildExecutorConf(workerNumCores, workerMemSize, centCommConfProvider);
    this.vertexTableConf = buildVertexTableConf(dataParser, vertexValueCodec, edgeCodec, VERTEX_TABLE_ID);
    this.msgTable1Conf = buildMsgTableConf(msgValueCodec, MSG_TABLE_1_ID);
    this.msgTable2Conf = buildMsgTableConf(msgValueCodec, MSG_TABLE_2_ID);
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime startTime) {

      final List<AllocatedExecutor> executors;

      try {
        executors = etMaster.addExecutors(numWorkers, executorConf).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }

      new Thread(() -> {
        try {
          final AllocatedTable msgTable1 = etMaster.createTable(msgTable1Conf, executors).get();
          final AllocatedTable msgTable2 = etMaster.createTable(msgTable2Conf, executors).get();
          final AllocatedTable vertexTable = etMaster.createTable(vertexTableConf, executors).get();

          vertexTable.load(executors, inputDir).get();

          pregelMaster.start(executors, vertexTable, msgTable1, msgTable2);

          executors.forEach(AllocatedExecutor::close);

        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }).start();
    }
  }

  private ExecutorConfiguration buildExecutorConf(final int workerNumCores,
                                                  final int workerMemSize,
                                                  final CentCommConfProvider centCommConfProvider) {
    return ExecutorConfiguration.newBuilder()
        .setResourceConf(ResourceConfiguration.newBuilder()
            .setNumCores(workerNumCores)
            .setMemSizeInMB(workerMemSize)
            .build())
        .setRemoteAccessConf(RemoteAccessConfiguration.newBuilder()
            .setHandlerQueueSize(2048)
            .setNumHandlerThreads(4)
            .setSenderQueueSize(2048)
            .setNumSenderThreads(4)
            .build())
        .setUserContextConf(centCommConfProvider.getContextConfiguration())
        .setUserServiceConf(centCommConfProvider.getServiceConfWithoutNameResolver())
        .build();
  }

  /**
   * Build a configuration of vertex table.
   * Need to provide codecs for vertex value and edge.
   *
   * @param tableId an identifier of {@link TableConfiguration}
   */
  private TableConfiguration buildVertexTableConf(final DataParser dataParser,
                                                  final StreamingCodec vertexValueCodec,
                                                  final StreamingCodec edgeCodec,
                                                  final String tableId) throws InjectionException {
    // configure vertex value codec, edge codec to vertex table
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
  private TableConfiguration buildMsgTableConf(final StreamingCodec messageValueCodec,
                                               final String tableId) throws InjectionException {
    // configure message value codec to message table
    final Configuration messageValueConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageValueCodec.class, messageValueCodec.getClass())
        .build();

    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(MessageCodec.class)
        .setUpdateValueCodecClass(((Codec) messageValueCodec).getClass())
        .setUpdateFunctionClass(MessageUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .setUserParamConf(messageValueConf)
        .build();
  }
}
