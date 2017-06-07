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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.services.et.evaluator.impl.VoidUpdateFunction;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Checks whether the FallbackManager redirects a table access request message to an Executor,
 * which is assumed to have failed in other Executors (mainly due to inconsistency in ownership status).
 *
 * The test is done as follows:
 * 1) A table initialized with one Associator (an executor whose id is "owner")
 * 2) The FallbackManager receives a TableAccessReqMsg, which is regarded as a retry from another Executor that is
 *   not an owner (an executor whose id is "non-owner")
 * 3) The test checks the information of the message sent from FallbackManager; especially the destination should be
 *   "owner".
 */
public final class FallbackManagerTest {

  private static final int NUM_TOTAL_BLOCKS = 1024;

  private static final String OWNER_EXECUTOR_ID = "owner";
  private static final String NON_OWNER_EXECUTOR_ID = "non-owner";
  private static final String TABLE_ID = "table";
  private static final long OP_ID = 0;
  private static final String KEY = "key";

  private MessageSender msgSender;
  private FallbackManager fallbackManager;

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(MessageSender.class, mock(MessageSender.class));
    msgSender = injector.getInstance(MessageSender.class);

    final TableManager tableManager = injector.getInstance(TableManager.class);

    final AllocatedExecutor ownerExecutor = mock(AllocatedExecutor.class);
    when(ownerExecutor.getId()).thenReturn(OWNER_EXECUTOR_ID);
    final List<AllocatedExecutor> ownerExecutors = new ArrayList<>();
    ownerExecutors.add(ownerExecutor);

    tableManager.createTable(
        TableConfiguration.newBuilder()
            .setId(TABLE_ID)
            .setIsMutableTable(false)
            .setIsOrderedTable(false)
            .setUpdateFunctionClass(VoidUpdateFunction.class)
            .setUpdateValueCodecClass(SerializableCodec.class)
            .setKeyCodecClass(SerializableCodec.class)
            .setValueCodecClass(SerializableCodec.class)
            .setNumTotalBlocks(NUM_TOTAL_BLOCKS)
            .build(),
        ownerExecutors);
    fallbackManager = injector.getInstance(FallbackManager.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFallback() throws InjectionException, NetworkException {
    final Codec codec = Tang.Factory.getTang().newInjector().getInstance(SerializableCodec.class);

    // Any operation will give the same results; GET is used here for simplicity.
    final TableAccessReqMsg getMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(NON_OWNER_EXECUTOR_ID)
        .setOpType(OpType.GET)
        .setTableId(TABLE_ID)
        .setDataKey(
            DataKey.newBuilder()
                .setKey(ByteBuffer.wrap(codec.encode(KEY)))
                .build())
        .setDataValue(null)
        .build();

    final TableAccessMsg tableAccessMsg = TableAccessMsg.newBuilder()
        .setType(TableAccessMsgType.TableAccessReqMsg)
        .setOperationId(OP_ID)
        .setTableAccessReqMsg(getMsg)
        .build();

    // Assume that the executor "non-owner" has redirected the message since the executor
    // has no information regarding the table.
    fallbackManager.onTableAccessReqMessage(NON_OWNER_EXECUTOR_ID, tableAccessMsg);

    doAnswer(invocation -> {
      final Object[] arguments = invocation.getArguments();

      final String destId = (String) arguments[0];
      final long opId = (long) arguments[1];
      final TableAccessReqMsg tableAccessReqMsg = (TableAccessReqMsg) arguments[2];

      assertEquals("The original executor is different", NON_OWNER_EXECUTOR_ID, tableAccessReqMsg.getOrigId());
      assertEquals("The destination is different", OWNER_EXECUTOR_ID, destId);
      assertEquals("The op id is different", OP_ID, opId);
      assertEquals("The table id is different", TABLE_ID, tableAccessReqMsg.getTableId());
      return null;
    }).when(msgSender).sendTableAccessReqMsg(anyString(), anyLong(), any(TableAccessReqMsg.class));
  }
}
