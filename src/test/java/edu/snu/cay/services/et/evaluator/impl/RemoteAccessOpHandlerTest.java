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
import edu.snu.cay.services.et.avro.TableAccessReqMsg;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import edu.snu.cay.services.et.driver.impl.BlockManager;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.examples.addinteger.AddIntegerUpdateFunction;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.StreamingSerializableCodec;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.group.impl.utils.ResettingCountDownLatch;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link RemoteAccessOpHandler}.
 * For simplicity, the test uses one table owned by one executor.
 */
public final class RemoteAccessOpHandlerTest {
  private static final Logger LOG = Logger.getLogger(RemoteAccessOpHandlerTest.class.getName());

  private static final String TABLE_ID = "Table";
  private static final int NUM_TOTAL_BLOCKS = 1024;
  private static final String TARGET_EXECUTOR_ID = "executor-0";
  private static final String ORIG_EXECUTOR_ID = "executor-1";
  private static final String DRIVER_ID = "Driver";

  private RemoteAccessOpHandler remoteAccessOpHandler;
  private Tables tables;
  private MessageSender mockMsgSender;

  @Before
  public void setup() throws InjectionException {
    final Configuration driverConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_TOTAL_BLOCKS))
        .build();

    final Injector driverInjector = Tang.Factory.getTang().newInjector(driverConf);
    final BlockManager blockManager = driverInjector.getInstance(BlockManager.class);
    final Set<String> executorIds = new HashSet<>();
    executorIds.add(TARGET_EXECUTOR_ID); // one executor owns whole blocks
    blockManager.init(executorIds);
    final List<String> blockOwners = blockManager.getOwnershipStatus();

    final Configuration evalConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ExecutorIdentifier.class, TARGET_EXECUTOR_ID)
        .bindNamedParameter(DriverIdentifier.class, DRIVER_ID)
        .build();

    mockMsgSender = mock(MessageSender.class);

    final Injector evalInjector = Tang.Factory.getTang().newInjector(evalConf);
    evalInjector.bindVolatileInstance(MessageSender.class, mockMsgSender);
    remoteAccessOpHandler = evalInjector.getInstance(RemoteAccessOpHandler.class);

    tables = evalInjector.getInstance(Tables.class);
    tables.initTable(buildTableConf(TABLE_ID, NUM_TOTAL_BLOCKS).getConfiguration(), blockOwners);
  }

  private TableConfiguration buildTableConf(final String tableId, final int numTotalBlocks) {
    return TableConfiguration.newBuilder()
        .setId(tableId)
        .setNumTotalBlocks(numTotalBlocks)
        .setKeyCodecClass(StreamingSerializableCodec.class)
        .setValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateValueCodecClass(StreamingSerializableCodec.class)
        .setUpdateFunctionClass(AddIntegerUpdateFunction.class)
        .setIsMutableTable(true)
        .setIsOrderedTable(false)
        .build();
  }

  @Test
  public void testRemoteAccess() throws TableNotExistException, InterruptedException, NetworkException {
    final TableComponents<String, Integer, Integer> tableComponents = tables.getTableComponents(TABLE_ID);
    final KVUSerializer<String, Integer, Integer> kvuSerializer = tableComponents.getSerializer();
    final Codec<String> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<Integer> valueCodec = kvuSerializer.getValueCodec();

    // we can know whether the access has been finished using 'replyLatch'
    final ResettingCountDownLatch replyLatch = new ResettingCountDownLatch(1);
    // and can get the result through 'reply'
    final AtomicReference<Integer> reply = new AtomicReference<>();

    doAnswer(invocation -> {
      final Object[] arguments = invocation.getArguments();
      final String destId = (String) arguments[0];
      final long opId = (long) arguments[1];
      final DataValue dataValue = (DataValue) arguments[3];
      final Integer value = dataValue == null ? null : valueCodec.decode(dataValue.getValue().array());
      final boolean isSuccess = (boolean) arguments[4];

      if (!isSuccess || !destId.equals(ORIG_EXECUTOR_ID)) {
        fail(String.format("Operation with id %d is failed", opId));
      }

      reply.set(value);
      replyLatch.countDown();

      LOG.log(Level.FINE, "Access result. opId: {0}, value: {1}, destId: {2}",
          new Object[]{opId, value, destId});
      return null;
    }).when(mockMsgSender).sendTableAccessResMsg(anyString(), anyLong(), anyString(), anyObject(), anyBoolean());

    final String key = "key";
    final Integer value = 1;

    final Pair<DataKey, DataValue> dataPair = getDataPair(key, value, keyCodec, valueCodec);

    // 1. put value
    final AtomicInteger opIdCounter = new AtomicInteger(0);
    final TableAccessReqMsg putMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.PUT)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(dataPair.getValue())
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), putMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(null, reply.get());

    // 2. get value to confirm that put request has been done successfully
    final TableAccessReqMsg getMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.GET)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(null)
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), getMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(value, reply.get());

    // 3. update value based on the value put by the first request
    final TableAccessReqMsg updateMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.UPDATE)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(dataPair.getValue())
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), updateMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handled
    assertEquals(Integer.valueOf(value + value), reply.get());

    // 4. check that putIfAbsent request is ignored when there exists the associated value
    final TableAccessReqMsg putIfAbsentMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.PUT_IF_ABSENT)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(dataPair.getValue())
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), putIfAbsentMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(Integer.valueOf(value + value), reply.get()); // putIfAbsent cannot overwrite value

    // 5. get value to confirm that value does not change by putIfAbsent request
    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), getMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(Integer.valueOf(value + value), reply.get());

    // 6. remove value
    final TableAccessReqMsg removeMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.REMOVE)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(null)
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), removeMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(Integer.valueOf(value + value), reply.get());

    // 7. get value to confirm that value has been deleted by remove request
    remoteAccessOpHandler.onTableAccessReqMsg(opIdCounter.getAndIncrement(), getMsg);
    replyLatch.awaitAndReset(1); // replyLatch must have been released while the event was handle
    assertEquals(null, reply.get());

    verify(mockMsgSender, times(7))
        .sendTableAccessResMsg(anyString(), anyLong(), anyString(), anyObject(), anyBoolean());
  }

  @Test
  public void testFallback() throws TableNotExistException, NetworkException {
    final long origOpId = 0;

    doAnswer(invocation -> {
      final Object[] arguments = invocation.getArguments();
      final String origId = (String) arguments[0];
      final String destId = (String) arguments[1];
      final long opId = (long) arguments[2];

      assertEquals("The origin ID should be kept same", origId, ORIG_EXECUTOR_ID);
      assertEquals("The request should be redirected to the driver", DRIVER_ID, destId);
      assertEquals("The op ID should be kept same", origOpId, opId);
      return null;
    }).when(mockMsgSender).sendTableAccessReqMsg(anyString(), anyString(), anyLong(), anyString(),
        any(OpType.class), anyBoolean(), any(DataKey.class), anyObject());

    final TableComponents<String, Integer, Integer> tableComponents = tables.getTableComponents(TABLE_ID);
    final KVUSerializer<String, Integer, Integer> kvuSerializer = tableComponents.getSerializer();
    final Codec<String> keyCodec = kvuSerializer.getKeyCodec();
    final Codec<Integer> valueCodec = kvuSerializer.getValueCodec();

    // Assume that the table has been unassociated
    tables.remove(TABLE_ID);

    // Any operation will give the same results; GET is used here for simplicity.
    final String key = "key";
    final Integer value = 1;
    final Pair<DataKey, DataValue> dataPair = getDataPair(key, value, keyCodec, valueCodec);

    final TableAccessReqMsg getMsg = TableAccessReqMsg.newBuilder()
        .setOrigId(ORIG_EXECUTOR_ID)
        .setOpType(OpType.GET)
        .setTableId(TABLE_ID)
        .setDataKey(dataPair.getKey())
        .setDataValue(null)
        .build();

    remoteAccessOpHandler.onTableAccessReqMsg(origOpId, getMsg);
  }

  private <K, V> Pair<DataKey, DataValue> getDataPair(final K key, final V value,
                                                      final Codec<K> keyCodec,
                                                      final Codec<V> valueCodec) {
    final DataKey dataKey = new DataKey();
    dataKey.setKey(ByteBuffer.wrap(keyCodec.encode(key)));

    final DataValue dataValue;
    if (value != null) {
      dataValue = new DataValue();
      dataValue.setValue(ByteBuffer.wrap(valueCodec.encode(value)));
    } else {
      dataValue = null;
    }
    return Pair.of(dataKey, dataValue);
  }
}
