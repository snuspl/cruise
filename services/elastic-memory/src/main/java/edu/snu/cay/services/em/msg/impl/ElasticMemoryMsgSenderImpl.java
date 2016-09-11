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
import org.htrace.Span;
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
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Sender class that uses NetworkConnectionService to
 * send AvroElasticMemoryMessages to the driver and evaluators.
 */
public final class ElasticMemoryMsgSenderImpl implements ElasticMemoryMsgSender {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgSenderImpl.class.getName());

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
  public void sendRemoteOpReqMsg(final String origId, final String destId, final DataOpType operationType,
                                 final List<KeyRange> dataKeyRanges,
                                 final List<KeyValuePair> dataKVPairList, final String operationId,
                                 @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRemoteOpReqMsgScope = Trace.startSpan("send_remote_op_req_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpReqMsg", new Object[]{destId,
          operationType});

      // the operation begins with a local client, when the origId is null
      final String origEvalId = origId == null ? emNetworkSetup.getMyId().toString() : origId;

      detached = sendRemoteOpReqMsgScope.detach();

      send(destId, generateRemoteOpReqMsg(origEvalId, destId, operationType,
          dataKeyRanges, dataKVPairList, operationId, TraceInfo.fromSpan(detached)));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpReqMsg", new Object[]{destId,
          operationType});
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendRemoteOpReqMsg(final String origId, final String destId, final DataOpType operationType,
                                 final DataKey dataKey, final DataValue dataValue, final String operationId,
                                 @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRemoteOpReqMsgScope = Trace.startSpan("send_remote_op_req_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpReqMsg", new Object[]{destId,
          operationType});

      // the operation begins with a local client, when the origId is null
      final String origEvalId = origId == null ? emNetworkSetup.getMyId().toString() : origId;

      detached = sendRemoteOpReqMsgScope.detach();

      send(destId, generateRemoteOpReqMsg(origEvalId, destId, operationType,
          dataKey, dataValue, operationId, TraceInfo.fromSpan(detached)));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpReqMsg", new Object[]{destId,
          operationType});
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  private AvroElasticMemoryMessage generateRemoteOpReqMsg(final String origId, final String destId,
                                                          final DataOpType operationType,
                                                          final Object dataKeys, final Object dataValues,
                                                          final String operationId,
                                                          @Nullable final TraceInfo parentTraceInfo) {
    final RemoteOpReqMsg remoteOpReqMsg = RemoteOpReqMsg.newBuilder()
        .setOrigEvalId(origId)
        .setOpType(operationType)
        .setDataKeys(dataKeys)
        .setDataValues(dataValues)
        .build();

    return AvroElasticMemoryMessage.newBuilder()
        .setType(Type.RemoteOpReqMsg)
        .setSrcId(emNetworkSetup.getMyId().toString())
        .setDestId(destId)
        .setOperationId(operationId)
        .setTraceInfo(HTraceUtils.toAvro(parentTraceInfo))
        .setRemoteOpReqMsg(remoteOpReqMsg)
        .build();
  }

  @Override
  public void sendRemoteOpResultMsg(final String destId, final List<KeyValuePair> dataKVPairList,
                                    final List<KeyRange> failedRanges, final String operationId,
                                    @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRemoteOpResultMsgScope = Trace.startSpan("send_remote_op_result_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);

      final boolean isSuccess = failedRanges.isEmpty();

      detached = sendRemoteOpResultMsgScope.detach();

      send(destId,
          generateRemoteOpResultMsg(destId, dataKVPairList, isSuccess, failedRanges, operationId,
              TraceInfo.fromSpan(detached)));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendRemoteOpResultMsg(final String destId, final DataValue dataValue, final boolean isSuccess,
                                    final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRemoteOpResultMsgScope = Trace.startSpan("send_remote_op_result_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);

      detached = sendRemoteOpResultMsgScope.detach();

      send(destId,
          generateRemoteOpResultMsg(destId, dataValue, isSuccess, null, operationId,
              TraceInfo.fromSpan(detached)));

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRemoteOpResultMsg", destId);
    } finally {
      Trace.continueSpan(detached).close();
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

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRoutingInitReqMsgScope =
             Trace.startSpan("send_routing_table_init_req_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitReqMsg");

      final RoutingTableInitReqMsg routingTableInitReqMsg = RoutingTableInitReqMsg.newBuilder()
          .build();

      detached = sendRoutingInitReqMsgScope.detach();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableInitReqMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .setRoutingTableInitReqMsg(routingTableInitReqMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitReqMsg");
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendRoutingTableInitMsg(final String destId, final List<Integer> blockLocations,
                                      @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRoutingTableInitMsgScope =
             Trace.startSpan("send_routing_table_init_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitMsg");

      final RoutingTableInitMsg routingTableInitMsg = RoutingTableInitMsg.newBuilder()
          .setBlockLocations(blockLocations)
          .build();

      detached = sendRoutingTableInitMsgScope.detach();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableInitMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .setRoutingTableInitMsg(routingTableInitMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableInitMsg");
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendRoutingTableUpdateMsg(final String destId, final List<Integer> blocks,
                                        final String oldEvalId, final String newEvalId,
                                        @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendRoutingTableUpdateMsgScope =
             Trace.startSpan("send_routing_table_update_msg" + ". destId: " + destId, parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableUpdateMsg");

      final RoutingTableUpdateMsg routingTableUpdateMsg = RoutingTableUpdateMsg.newBuilder()
          .setOldEvalId(oldEvalId)
          .setNewEvalId(newEvalId)
          .setBlockIds(blocks)
          .build();

      detached = sendRoutingTableUpdateMsgScope.detach();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.RoutingTableUpdateMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .setRoutingTableUpdateMsg(routingTableUpdateMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendRoutingTableUpdateMsg");

    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendCtrlMsg(final String destId, final String targetEvalId,
                          final List<Integer> blocks, final String operationId,
                          @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    // sending ctrl msg is the starting point of the migration protocol
    try (final TraceScope sendCtrlMsgScope = Trace.startSpan("[1]send_ctrl_msg", parentTraceInfo)) {

      final CtrlMsg ctrlMsg = CtrlMsg.newBuilder()
          .setBlockIds(blocks)
          .build();

      detached = sendCtrlMsgScope.detach();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.CtrlMsg)
              .setSrcId(destId)
              .setDestId(targetEvalId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .setCtrlMsg(ctrlMsg)
              .build());
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendDataMsg(final String destId, final List<KeyValuePair> keyValuePairs, final int blockId,
                          final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    int totalKeyBytes = 0;
    int totalValueBytes = 0;

    for (final KeyValuePair keyValuePair : keyValuePairs) {
      final int keyByteLength = keyValuePair.getKey().array().length;
      final int valueByteLength = keyValuePair.getValue().array().length;
      totalKeyBytes += keyByteLength;
      totalValueBytes += valueByteLength;
    }

    LOG.log(Level.INFO, "SendDataMsg: op_id: {0}, dest_id: {1}, block_id: {2}," +
        " num_kv_pairs: {3}, k_bytes: {4}, v_bytes: {5}",
        new Object[]{operationId, destId, blockId, keyValuePairs.size(), totalKeyBytes, totalValueBytes});

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    // sending data msg is the second step of the migration protocol
    try (final TraceScope sendDataMsgScope = Trace.startSpan("[2]send_data_msg"
        + String.format(". op_id: %s, dest: %s, block_id: %d, num_kv_pairs: %d, (k_bytes, v_bytes): (%d, %d)",
        operationId, destId, blockId, keyValuePairs.size(), totalKeyBytes, totalValueBytes), parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg",
          new Object[]{destId});

      final DataMsg dataMsg = DataMsg.newBuilder()
          .setKeyValuePairs(keyValuePairs)
          .setBlockId(blockId)
          .build();

      detached = sendDataMsgScope.detach();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.DataMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .setDataMsg(dataMsg)
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendDataMsg",
          new Object[]{destId});
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendOwnershipMsg(final Optional<String> destIdOptional, final String operationId,
                               final int blockId, final int oldOwnerId, final int newOwnerId,
                               @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    // sending ownership msg to driver is the third step and to src eval is the fourth step of the migration protocol
    final String destId = destIdOptional.isPresent() ? destIdOptional.get() : driverId;
    final String traceSpanPrefix = destIdOptional.isPresent() ? "[4]" : "[3]";

    try (final TraceScope sendOwnershipMsgScope = Trace.startSpan(traceSpanPrefix + "send_ownership_msg"
        + ". blockId: " + blockId, parentTraceInfo)) {
      final OwnershipMsg ownershipMsg =
          OwnershipMsg.newBuilder()
              .setBlockId(blockId)
              .setOldOwnerId(oldOwnerId)
              .setNewOwnerId(newOwnerId)
              .build();

      detached = sendOwnershipMsgScope.detach();

      send(destId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.OwnershipMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(destId)
              .setOperationId(operationId)
              .setOwnershipMsg(ownershipMsg)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .build());
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendOwnershipAckMsg(final String operationId, final int blockId,
                                  @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendOwnershipAckMsgScope = Trace.startSpan("[5]send_ownership_ack_msg"
        + ". blockId: " + blockId, parentTraceInfo)) {
      final OwnershipAckMsg ownershipAckMsg = OwnershipAckMsg.newBuilder()
          .setBlockId(blockId)
          .build();

      detached = sendOwnershipAckMsgScope.detach();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.OwnershipAckMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setOwnershipAckMsg(ownershipAckMsg)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .build());
    } finally {
      Trace.continueSpan(detached).close();
    }
  }

  @Override
  public void sendFailureMsg(final String operationId, final String reason, @Nullable final TraceInfo parentTraceInfo) {

    // We should detach the span when we transit to another thread (local or remote),
    // and the detached span should call Trace.continueSpan(detached).close() explicitly
    // for stitching the spans from other threads as its children
    Span detached = null;

    try (final TraceScope sendFailureMsgScope = Trace.startSpan("send_failure_msg", parentTraceInfo)) {

      LOG.entering(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendFailureMsg",
          new Object[]{operationId, reason});

      final FailureMsg failureMsg =
          FailureMsg.newBuilder()
              .setOperationId(operationId)
              .setReason(reason)
              .build();

      detached = sendFailureMsgScope.detach();

      send(driverId,
          AvroElasticMemoryMessage.newBuilder()
              .setType(Type.FailureMsg)
              .setSrcId(emNetworkSetup.getMyId().toString())
              .setDestId(driverId)
              .setOperationId(operationId)
              .setFailureMsg(failureMsg)
              .setTraceInfo(HTraceUtils.toAvro(TraceInfo.fromSpan(detached)))
              .build());

      LOG.exiting(ElasticMemoryMsgSenderImpl.class.getSimpleName(), "sendFailureMsg",
          new Object[]{operationId, reason});
    } finally {
      Trace.continueSpan(detached).close();
    }
  }
}
