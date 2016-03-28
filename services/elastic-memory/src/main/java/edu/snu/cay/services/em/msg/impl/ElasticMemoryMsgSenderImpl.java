/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.em.msg.impl;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.ns.EMNetworkSetup;
import edu.snu.cay.services.em.utils.AvroUtils;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.commons.lang.math.LongRange;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.IdentifierFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;


/**
 * Sender class that uses NetworkConnectionService to
 * send AvroElasticMemoryMessages to the driver and evaluators.
 */
public final class ElasticMemoryMsgSenderImpl implements ElasticMemoryMsgSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgSenderImpl.class.getName());

  private static final String SEND_REMOTE_OP_MSG = "sendRemoteOpMsg";
  private static final String SEND_REMOTE_OP_RESULT_MSG = "sendRemoteOpResultMsg";
  private static final String SEND_CTRL_MSG = "sendCtrlMsg";
  private static final String SEND_DATA_MSG = "sendDataMsg";
  private static final String SEND_DATA_ACK_MSG = "sendDataAckMsg";
  private static final String SEND_REGIS_MSG = "sendRegisMsg";
  private static final String SEND_UPDATE_MSG = "sendUpdateMsg";
  private static final String SEND_UPDATE_ACK_MSG = "sendUpdateAckMsg";
  private static final String SEND_FAILURE_MSG = "sendFailureMsg";

  private final EMNetworkSetup emNetworkSetup;
  private final IdentifierFactory identifierFactory;

  private final String driverId;

  @Inject
  private ElasticMemoryMsgSenderImpl(final EMNetworkSetup emNetworkSetup,
                                     final IdentifierFactory identifierFactory,
                                     @Parameter(DriverIdentifier.class) final String driverId) throws NetworkException {
    this.emNetworkSetup = emNetworkSetup;
    this.identifierFactory = identifierFactory;

    this.driverId = driverId;
  }

  private void send(final String destId, final AvroElasticMemoryMessage msg) {
    LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "send", new Object[]{destId, msg});

    final Connection<AvroElasticMemoryMessage> conn = emNetworkSetup.getConnectionFactory()
        .newConnection(identifierFactory.getNewInstance(destId));
    try {
      conn.open();
      conn.write(msg);
    } catch (final NetworkException ex) {
      // TODO #90: Revisit how to react to network failures. This can bubble up to the PlanExecutor.
      throw new RuntimeException("NetworkException", ex);
    }

    LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "send", new Object[]{destId, msg});
  }


  @Override
  public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                              final String dataType, final long dataKey, final ByteBuffer inputData,
                              final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpMsgScope = Trace.startSpan(SEND_REMOTE_OP_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType, dataKey, inputData});

      // the operation begins with a local client, when the origId is null
      final String origEvalId = origId == null ? emNetworkSetup.getMyId().toString() : origId;

      final RemoteOpMsg remoteOpMsg = RemoteOpMsg.newBuilder()
          .setOrigEvalId(origEvalId)
          .setOpType(operationType)
          .setDataType(dataType)
          .setDataKey(dataKey)
          .setDataValue(inputData)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RemoteOpMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setRemoteOpMsg(remoteOpMsg)
              .build()
      );

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType, dataKey});
    }
  }

  @Override
  public void sendRemoteOpResultMsg(final String destId, final boolean isSuccess, final ByteBuffer outputData,
                                    final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpResultMsgScope = Trace.startSpan(SEND_REMOTE_OP_RESULT_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", new Object[]{destId,
          isSuccess, outputData});

      final RemoteOpResultMsg remoteOpResultMsg = RemoteOpResultMsg.newBuilder()
          .setIsSuccess(isSuccess)
          .setDataValue(outputData)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RemoteOpResultMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setRemoteOpResultMsg(remoteOpResultMsg)
              .build()
      );

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", new Object[]{destId,
          isSuccess, outputData});
    }
  }

  @Override
  public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                          final Set<LongRange> idRangeSet, final String operationId, final TraceInfo parentTraceInfo) {
    try (final TraceScope sendCtrlMsgScope = Trace.startSpan(SEND_CTRL_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendCtrlMsg",
          new Object[]{destId, dataType, targetEvalId, idRangeSet});

      final List<AvroLongRange> avroLongRangeList = new LinkedList<>();
      for (final LongRange idRange : idRangeSet) {
        avroLongRangeList.add(AvroUtils.toAvroLongRange(idRange));
      }

      final CtrlMsg ctrlMsg = CtrlMsg.newBuilder()
          .setDataType(dataType)
          .setCtrlMsgType(CtrlMsgType.IdRange)
          .setIdRange(avroLongRangeList)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.CtrlMsg)
              .setSrcId(destId)
              .setDestId(targetEvalId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setCtrlMsg(ctrlMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendCtrlMsg",
          new Object[]{destId, dataType, targetEvalId});
    }
  }

  @Override
  public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                          final int numUnits, final String operationId, final TraceInfo parentTraceInfo) {
    try (final TraceScope sendCtrlMsgScope = Trace.startSpan(SEND_CTRL_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendCtrlMsg",
          new Object[]{destId, dataType, targetEvalId, numUnits});

      final CtrlMsg ctrlMsg = CtrlMsg.newBuilder()
          .setDataType(dataType)
          .setCtrlMsgType(CtrlMsgType.NumUnits)
          .setNumUnits(numUnits)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.CtrlMsg)
              .setSrcId(destId)
              .setDestId(targetEvalId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setCtrlMsg(ctrlMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendCtrlMsg",
          new Object[]{destId, dataType, targetEvalId});
    }
  }

  @Override
  public void sendDataMsg(final String destId, final String dataType, final List<UnitIdPair> unitIdPairList,
                          final String operationId, final TraceInfo parentTraceInfo) {
    try (final TraceScope sendDataMsgScope = Trace.startSpan(SEND_DATA_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg",
          new Object[]{destId, dataType});

      final DataMsg dataMsg = DataMsg.newBuilder()
          .setDataType(dataType)
          .setUnits(unitIdPairList)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.DataMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setDataMsg(dataMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg",
          new Object[]{destId, dataType});
    }
  }

  @Override
  public void sendDataAckMsg(final Set<LongRange> idRangeSet,
                             final String operationId, final TraceInfo parentTraceInfo) {
    try (final TraceScope sendDataAckMsgScope = Trace.startSpan(SEND_DATA_ACK_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataAckMsg", new Object[]{operationId});

      final List<AvroLongRange> avroLongRanges = new ArrayList<>(idRangeSet.size());
      for (final LongRange range : idRangeSet) {
        avroLongRanges.add(AvroUtils.toAvroLongRange(range));
      }

      final DataAckMsg dataAckMsg = DataAckMsg.newBuilder()
          .setIdRange(avroLongRanges)
          .build();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.DataAckMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setDataAckMsg(dataAckMsg)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataAckMsg", new Object[]{operationId});

    }
  }

  @Override
  public void sendRegisMsg(final String dataType, final long unitStartId, final long unitEndId,
                           final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRegisMsgScope = Trace.startSpan(SEND_REGIS_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRegisMsg",
          new Object[]{dataType, unitStartId, unitEndId});

      final RegisMsg regisMsg = RegisMsg.newBuilder()
          .setDataType(dataType)
          .setIdRange(AvroUtils.toAvroLongRange(new LongRange(unitStartId, unitEndId)))
          .build();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RegisMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setRegisMsg(regisMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRegisMsg",
          new Object[]{dataType, unitStartId, unitEndId});
    }
  }

  @Override
  public void sendUpdateMsg(final String destId, final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendUpdateMsgScope = Trace.startSpan(SEND_UPDATE_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendUpdateMsg",
          new Object[]{destId});

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.UpdateMsg)
              .setSrcId(driverId)
              .setDestId(destId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendUpdateMsg",
          new Object[]{});
    }
  }

  @Override
  public void sendUpdateAckMsg(final String operationId,
                               final UpdateResult result,
                               @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendUpdateAckMsgScope = Trace.startSpan(SEND_UPDATE_ACK_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendUpdateAckMsg",
          new Object[]{operationId, result});

      final UpdateAckMsg updateAckMsg =
          UpdateAckMsg.newBuilder()
              .setResult(result)
              .build();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.UpdateAckMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setUpdateAckMsg(updateAckMsg)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendUpdateAckMsg",
          new Object[]{operationId, result});
    }
  }

  @Override
  public void sendFailureMsg(final String operationId, final String reason, @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendFailureMsgScope = Trace.startSpan(SEND_FAILURE_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendFailureMsg",
          new Object[]{operationId, reason});

      final FailureMsg failureMsg =
          FailureMsg.newBuilder()
              .setOperationId(operationId)
              .setReason(reason)
              .build();
      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.FailureMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setFailureMsg(failureMsg)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendFailureMsg",
          new Object[]{operationId, reason});
    }
  }
}
