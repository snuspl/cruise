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
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.util.Optional;
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
import java.util.*;
import java.util.logging.Logger;


/**
 * Sender class that uses NetworkConnectionService to
 * send AvroElasticMemoryMessages to the driver and evaluators.
 */
public final class ElasticMemoryMsgSenderImpl implements ElasticMemoryMsgSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgSenderImpl.class.getName());

  private static final String SEND_REMOTE_OP_MSG = "sendRemoteOpMsg";
  private static final String SEND_REMOTE_OP_RESULT_MSG = "sendRemoteOpResultMsg";
  private static final String SEND_ROUTING_TABLE_INIT_REQ_MSG = "sendRoutingTableInitReqMsg";
  private static final String SEND_ROUTING_TABLE_INIT_MSG = "sendRoutingTableInitMsg";
  private static final String SEND_ROUTING_TABLE_UPDATE_MSG = "sendRoutingTableUpdateMsg";
  private static final String SEND_CTRL_MSG = "sendCtrlMsg";
  private static final String SEND_DATA_MSG = "sendDataMsg";
  private static final String SEND_OWNERSHIP_MSG = "sendOwnershipMsg";
  private static final String SEND_FAILURE_MSG = "sendFailureMsg";
  private static final String SEND_OWNERSHIP_ACK_MSG = "sendOwnershipAckMsg";

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
                              final String dataType, final List<KeyRange> dataKeyRanges,
                              final List<KeyValuePair> dataKVPairList, final String operationId,
                              @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpMsgScope = Trace.startSpan(SEND_REMOTE_OP_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType});

      // the operation begins with a local client, when the origId is null
      final String origEvalId = origId == null ? emNetworkSetup.getMyId().toString() : origId;

      send(destId, generateRemoteOpMsg(origEvalId, destId, operationType, dataType,
          dataKeyRanges, dataKVPairList, operationId, parentTraceInfo));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType});
    }
  }

  @Override
  public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                              final String dataType, final DataKey dataKey,
                              final DataValue dataValue, final String operationId,
                              @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpMsgScope = Trace.startSpan(SEND_REMOTE_OP_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType});

      // the operation begins with a local client, when the origId is null
      final String origEvalId = origId == null ? emNetworkSetup.getMyId().toString() : origId;

      send(destId, generateRemoteOpMsg(origEvalId, destId, operationType, dataType,
          dataKey, dataValue, operationId, parentTraceInfo));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpMsg", new Object[]{destId,
          operationType, dataType});
    }
  }

  private AvroElasticMemoryMessage generateRemoteOpMsg(final String origId, final String destId,
                                                       final DataOpType operationType, final String dataType,
                                                       final Object dataKeys, final Object dataValues,
                                                       final String operationId,
                                                       @Nullable final TraceInfo parentTraceInfo) {
    final RemoteOpMsg remoteOpMsg = RemoteOpMsg.newBuilder()
        .setOrigEvalId(origId)
        .setOpType(operationType)
        .setDataType(dataType)
        .setDataKeys(dataKeys)
        .setDataValues(dataValues)
        .build();

    return AvroElasticMemoryMessage.newBuilder()
        .setType(Type.RemoteOpMsg)
        .setSrcId(emNetworkSetup.getMyId().toString())
        .setDestId(destId)
        .setOperationId(operationId)
        .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
        .setRemoteOpMsg(remoteOpMsg)
        .build();
  }

  @Override
  public void sendRemoteOpResultMsg(final String destId, final List<KeyValuePair> dataKVPairList,
                                    final List<KeyRange> failedRanges, final String operationId,
                                    @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpResultMsgScope = Trace.startSpan(SEND_REMOTE_OP_RESULT_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);

      final boolean isSuccess = failedRanges.isEmpty();

      send(destId,
          generateRemoteOpResultMsg(destId, dataKVPairList, isSuccess, failedRanges, operationId, parentTraceInfo));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);
    }
  }

  @Override
  public void sendRemoteOpResultMsg(final String destId, final DataValue dataValue, final boolean isSuccess,
                                    final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRemoteOpResultMsgScope = Trace.startSpan(SEND_REMOTE_OP_RESULT_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);

      send(destId,
          generateRemoteOpResultMsg(destId, dataValue, isSuccess, null, operationId, parentTraceInfo));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);
    }

  }

  private AvroElasticMemoryMessage generateRemoteOpResultMsg(final String destId,
                                                             final Object dataValues, final boolean isSuccess,
                                                             final List<KeyRange> failedRanges,
                                                             final String operationId,
                                                             @Nullable final TraceInfo parentTraceInfo) {
    final RemoteOpResultMsg remoteOpResultMsg = RemoteOpResultMsg.newBuilder()
        .setIsSuccess(isSuccess)
        .setDataValues(dataValues)
        .setFailedKeyRanges(failedRanges)
        .build();

    return AvroElasticMemoryMessage.newBuilder()
        .setType(Type.RemoteOpResultMsg)
        .setSrcId(emNetworkSetup.getMyId().toString())
        .setDestId(destId)
        .setOperationId(operationId)
        .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
        .setRemoteOpResultMsg(remoteOpResultMsg)
        .build();
  }

  @Override
  public void sendRoutingTableInitReqMsg(@Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRoutingInitReqMsgScope =
             Trace.startSpan(SEND_ROUTING_TABLE_INIT_REQ_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitReqMsg");

      final RoutingTableInitReqMsg routingTableInitReqMsg = RoutingTableInitReqMsg.newBuilder()
          .build();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableInitReqMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setRoutingTableInitReqMsg(routingTableInitReqMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitReqMsg");
    }
  }

  @Override
  public void sendRoutingTableInitMsg(final String destId, final List<Integer> blockLocations,
                                      @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRoutingTableInitMsgScope =
             Trace.startSpan(SEND_ROUTING_TABLE_INIT_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitMsg");

      final RoutingTableInitMsg routingTableInitMsg = RoutingTableInitMsg.newBuilder()
          .setBlockLocations(blockLocations)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableInitMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setRoutingTableInitMsg(routingTableInitMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitMsg");
    }
  }

  @Override
  public void sendRoutingTableUpdateMsg(final String destId, final List<Integer> blocks,
                                        final String oldEvalId, final String newEvalId,
                                        @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendRoutingTableUpdateMsgScope =
             Trace.startSpan(SEND_ROUTING_TABLE_UPDATE_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableUpdateMsg");

      final RoutingTableUpdateMsg routingTableUpdateMsg = RoutingTableUpdateMsg.newBuilder()
          .setOldEvalId(oldEvalId)
          .setNewEvalId(newEvalId)
          .setBlockIds(blocks)
          .build();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableUpdateMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .setRoutingTableUpdateMsg(routingTableUpdateMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableUpdateMsg");
    }
  }

  @Override
  public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                          final List<Integer> blocks, final String operationId,
                          @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendCtrlMsgScope = Trace.startSpan(SEND_CTRL_MSG, parentTraceInfo)) {

      final CtrlMsg ctrlMsg = CtrlMsg.newBuilder()
          .setDataType(dataType)
          .setBlockIds(blocks)
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
    }
  }

  @Override
  public void sendDataMsg(final String destId, final String dataType,
                          final List<KeyValuePair> keyValuePairs, final int blockId, final String operationId,
                          final TraceInfo parentTraceInfo) {
    try (final TraceScope sendDataMsgScope = Trace.startSpan(SEND_DATA_MSG, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg",
          new Object[]{destId, dataType});

      final DataMsg dataMsg = DataMsg.newBuilder()
          .setDataType(dataType)
          .setKeyValuePairs(keyValuePairs)
          .setBlockId(blockId)
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
  public void sendOwnershipMsg(final Optional<String> destIdOptional, final String operationId, final String dataType,
                               final int blockId, final int oldOwnerId, final int newOwnerId,
                               @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendUpdateAckMsgScope = Trace.startSpan(SEND_OWNERSHIP_MSG, parentTraceInfo)) {
      final OwnershipMsg ownershipMsg =
          OwnershipMsg.newBuilder()
              .setDataType(dataType)
              .setBlockId(blockId)
              .setOldOwnerId(oldOwnerId)
              .setNewOwnerId(newOwnerId)
              .build();

      final String destId = destIdOptional.isPresent() ? destIdOptional.get() : driverId;

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.OwnershipMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setOwnershipMsg(ownershipMsg)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());
    }
  }

  @Override
  public void sendOwnershipAckMsg(final String operationId, final String dataType, final int blockId,
                                  @Nullable final TraceInfo parentTraceInfo) {
    try (final TraceScope sendOwnershipAckMsgScope = Trace.startSpan(SEND_OWNERSHIP_ACK_MSG, parentTraceInfo)) {
      final OwnershipAckMsg ownershipAckMsg = OwnershipAckMsg.newBuilder()
          .setDataType(dataType)
          .setBlockId(blockId)
          .build();
      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.OwnershipAckMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setOwnershipAckMsg(ownershipAckMsg)
              .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
              .build());
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
