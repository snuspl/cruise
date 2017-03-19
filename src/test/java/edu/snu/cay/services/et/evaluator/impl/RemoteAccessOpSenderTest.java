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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.DataKey;
import edu.snu.cay.services.et.avro.DataValue;
import edu.snu.cay.services.et.avro.OpType;
import edu.snu.cay.services.et.avro.TableAccessResMsg;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.driver.impl.BlockManager;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.examples.addinteger.AddIntegerUpdateFunction;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RemoteAccessOpSender}.
 */
public class RemoteAccessOpSenderTest {

  private static final String TABLE_ID = "Table";
  private static final String SENDER_ID = "executor-0";
  private static final String RECEIVER_ID = "executor-1";
  private static final int NUM_TOTAL_BLOCKS = 1024;

  private RemoteAccessOpSender remoteAccessOpSender;
  private MessageSender mockMsgSender;

  @Before
  public void setup() throws InjectionException, IOException, TableNotExistException {
    // 1. driver-side
    final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);
    final Set<String> executorIds = new HashSet<>();
    executorIds.add(RECEIVER_ID); // one executor owns whole blocks
    blockManager.init(executorIds);
    final List<String> blockOwners = blockManager.getOwnershipStatus();

    // 2. sender-side
    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ExecutorIdentifier.class, SENDER_ID)
        .build();

    mockMsgSender = mock(MessageSender.class);

    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);
    evalInjector.bindVolatileInstance(MessageSender.class, mockMsgSender);
    remoteAccessOpSender = evalInjector.getInstance(RemoteAccessOpSender.class);

    final Tables tables = evalInjector.getInstance(Tables.class);
    tables.initTable(buildTableConf(TABLE_ID, NUM_TOTAL_BLOCKS).getConfiguration(), blockOwners, null);
  }

  private TableConfiguration buildTableConf(final String tableId, final int numTotalBlocks) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(numTotalBlocks)
        .setKeyCodecClass(SerializableCodec.class)
        .setValueCodecClass(SerializableCodec.class)
        .setUpdateFunctionClass(AddIntegerUpdateFunction.class)
        .setIsOrderedTable(false)
        .build();
  }

  @Test
  public void testOpSend() throws TableNotExistException, InterruptedException {
    doAnswer(invocation -> {
      final long opId = invocation.getArgumentAt(2, Long.class);
      final DataValue dataValue = invocation.getArgumentAt(6, DataValue.class);

      final TableAccessResMsg tableAccessResMsg = TableAccessResMsg.newBuilder()
          .setDataValue(dataValue) // just set output value as its input
          .setIsSuccess(true)
          .build();

      remoteAccessOpSender.onTableAccessResMsg(opId, tableAccessResMsg);
      return null;
    }).when(mockMsgSender).sendTableAccessReqMsg(anyString(), anyString(), anyLong(), anyString(),
        any(OpType.class), any(DataKey.class), anyObject());

    final String key = "key";
    final Integer value = 1;

    final int blockId = 0; // block id means nothing here, so just set it as 0

    final RemoteDataOp<String, Integer> remoteDataOp =
        remoteAccessOpSender.sendOpToRemote(OpType.PUT, TABLE_ID, blockId, key, value, RECEIVER_ID);

    assertTrue("Operation should finish within timeout", remoteDataOp.waitRemoteOp(10000));
    assertTrue(remoteDataOp.isSuccess());
    assertEquals("output value should be same with input value", value, remoteDataOp.getOutputData());
  }
}
