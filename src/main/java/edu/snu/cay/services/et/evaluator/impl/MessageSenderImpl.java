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

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.utils.AvroUtils;
import edu.snu.cay.utils.HostnameResolver;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * A message sender implementation.
 */
@EvaluatorSide
public final class MessageSenderImpl implements MessageSender {
  private final NetworkConnection<ETMsg> networkConnection;

  private final String driverId;
  private final String executorId;

  @Inject
  private MessageSenderImpl(final NetworkConnection<ETMsg> networkConnection,
                            @Parameter(DriverIdentifier.class) final String driverId,
                            @Parameter(ExecutorIdentifier.class) final String executorId) {
    this.networkConnection = networkConnection;
    this.driverId = driverId;
    this.executorId = executorId;
  }

  @Override
  public void sendMsg(final String destId, final ETMsg etMsg) throws NetworkException {
    networkConnection.send(destId, etMsg);
  }

  @Override
  public void sendTableAccessReqMsg(final String origId, final String destId,
                                    final long opId, final String tableId,
                                    final OpType opType, final boolean replyRequired,
                                    final DataKey dataKey, @Nullable final DataValue dataValue)
      throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableAccessMsg.newBuilder()
            .setType(TableAccessMsgType.TableAccessReqMsg)
            .setOperationId(opId)
            .setTableAccessReqMsg(
                TableAccessReqMsg.newBuilder()
                    .setOrigId(origId)
                    .setTableId(tableId)
                    .setOpType(opType)
                    .setReplyRequired(replyRequired)
                    .setDataKey(dataKey)
                    .setDataValue(dataValue)
                    .build())
            .build(), TableAccessMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(destId, msg);
  }

  @Override
  public void sendTableAccessReqMsg(final String origId, final String destId,
                                    final long opId, final String tableId,
                                    final OpType opType, final boolean replyRequired,
                                    final DataKeys dataKeys, @Nullable final DataValues dataValues)
      throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableAccessMsg.newBuilder()
            .setType(TableAccessMsgType.TableAccessReqMsg)
            .setOperationId(opId)
            .setTableAccessReqMsg(
                TableAccessReqMsg.newBuilder()
                    .setOrigId(origId)
                    .setTableId(tableId)
                    .setOpType(opType)
                    .setReplyRequired(replyRequired)
                    .setIsSingleKey(false)
                    .setDataKeys(dataKeys)
                    .setDataValues(dataValues)
                    .build())
            .build(), TableAccessMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(destId, msg);

  }

  @Override
  public void sendTableAccessResMsg(final String destId, final long opId, final String tableId,
                                    @Nullable final DataValue dataValue, final boolean isSuccess)
      throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableAccessMsg.newBuilder()
            .setType(TableAccessMsgType.TableAccessResMsg)
            .setOperationId(opId)
            .setTableAccessResMsg(
                TableAccessResMsg.newBuilder()
                    .setIsSuccess(isSuccess)
                    .setTableId(tableId)
                    .setDataValue(dataValue)
                    .build())
            .build(), TableAccessMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(destId, msg);
  }

  @Override
  public void sendTableAccessResMsg(final String destId, final long opId, final String tableId,
                                    final DataKeys dataKeys, final DataValues dataValues, final boolean isSuccess)
      throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableAccessMsg.newBuilder()
            .setType(TableAccessMsgType.TableAccessResMsg)
            .setOperationId(opId)
            .setTableAccessResMsg(
                TableAccessResMsg.newBuilder()
                    .setIsSuccess(isSuccess)
                    .setDataKeys(dataKeys)
                    .setDataValues(dataValues)
                    .setTableId(tableId)
                    .build())
            .build(), TableAccessMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(destId, msg);
  }


  @Override
  public void sendTableInitAckMsg(final long opId, final String tableId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableInitAckMsg)
            .setOperationId(opId)
            .setTableInitAckMsg(
                TableInitAckMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setTableId(tableId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendTableLoadAckMsg(final long opId, final String tableId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableLoadAckMsg)
            .setOperationId(opId)
            .setTableLoadAckMsg(
                TableLoadAckMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setTableId(tableId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendTableDropAckMsg(final long opId, final String tableId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableDropAckMsg)
            .setOperationId(opId)
            .setTableDropAckMsg(
                TableDropAckMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setTableId(tableId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendChkpDoneMsg(final String chkpId, final List<Integer> blockIds) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableChkpMsg.newBuilder()
            .setType(TableChkpMsgType.ChkpDoneMsg)
            .setChkpId(chkpId)
            .setChkpDoneMsg(
                ChkpDoneMsg.newBuilder()
                .setExecutorId(executorId)
                .setBlockIds(blockIds)
                .build())
        .build(), TableChkpMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableChkpMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendChkpCommitMsg(final String chkpId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableChkpMsg.newBuilder()
            .setType(TableChkpMsgType.ChkpCommitMsg)
            .setChkpId(chkpId)
            .setChkpCommitMsg(
                ChkpCommitMsg.newBuilder()
                    .setExecutorId(executorId)
                    .build())
            .build(), TableChkpMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableChkpMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendChkpLoadDoneMsg(final String chkpId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableChkpMsg.newBuilder()
            .setType(TableChkpMsgType.ChkpLoadDoneMsg)
            .setChkpId(chkpId)
            .setChkpLoadDoneMsg(
                ChkpLoadDoneMsg.newBuilder()
                    .setExecutorId(executorId)
                    .build())
            .build(), TableChkpMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableChkpMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendOwnershipReqMsg(final String tableId, final int blockId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.OwnershipReqMsg)
            .setOwnershipReqMsg(
                OwnershipReqMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendOwnershipSyncAckMsg(final long opId, final String tableId,
                                      final String deletedExecutorId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.OwnershipSyncAckMsg)
            .setOperationId(opId)
            .setOwnershipSyncAckMsg(
                OwnershipSyncAckMsg.newBuilder()
                    .setTableId(tableId)
                    .setDeletedExecutorId(executorId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendOwnershipMsg(final long opId, final String tableId, final int blockId,
                               final String oldOwnerId, final String newOwnerId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.OwnershipMsg)
            .setOwnershipMsg(
                OwnershipMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setOldOwnerId(oldOwnerId)
                    .setNewOwnerId(newOwnerId)
                    .build())
            .build(), MigrationMsg.class);
    
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(newOwnerId, msg);
  }

  @Override
  public void sendOwnershipAckMsg(final long opId, final String tableId, final int blockId,
                                  final String oldOwnerId, final String newOwnerId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.OwnershipAckMsg)
            .setOwnershipAckMsg(
                OwnershipAckMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setOldOwnerId(oldOwnerId)
                    .setNewOwnerId(newOwnerId)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(oldOwnerId, msg);
  }

  @Override
  public void sendOwnershipMovedMsg(final long opId, final String tableId, final int blockId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.OwnershipMovedMsg)
            .setOwnershipMovedMsg(
                OwnershipMovedMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendDataMsg(final long opId, final String tableId, final int blockId,
                          final byte[] kvPairs, final int numItems, final int numTotalItems,
                          final String senderId, final String receiverId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.DataMsg)
            .setDataMsg(
                DataMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setNumItems(numItems)
                    .setNumTotalItems(numTotalItems)
                    .setKvPairs(ByteBuffer.wrap(kvPairs))
                    .setSenderId(senderId)
                    .setReceiverId(receiverId)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(receiverId, msg);
  }

  @Override
  public void sendDataAckMsg(final long opId, final String tableId, final int blockId,
                             final String senderId, final String receiverId) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.DataAckMsg)
            .setDataAckMsg(
                DataAckMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setSenderId(senderId)
                    .setReceiverId(receiverId)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(senderId, msg);
  }

  @Override
  public void sendDataMovedMsg(final long opId, final String tableId, final int blockId,
                               final boolean moveOwnershipTogether) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.DataMovedMsg)
            .setDataMovedMsg(
                DataMovedMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setMoveOwnershipTogether(moveOwnershipTogether)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendMetricReportMsg(final Map<String, Integer> tableToNumBlocks,
                                  final Map<String, Long> bytesReceivedGetResp,
                                  final Map<String, Integer> countSentGetReq,
                                  final List<ByteBuffer> encodedCustomMetrics) throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        MetricMsg.newBuilder()
            .setType(MetricMsgType.MetricReportMsg)
            .setMetricReportMsg(
                MetricReportMsg.newBuilder()
                    .setTableToNumBlocks(tableToNumBlocks)
                    .setBytesReceivedGetResp(bytesReceivedGetResp)
                    .setCountSentGetReq(countSentGetReq)
                    .setHostname(HostnameResolver.resolve())
                    .setCustomMetrics(encodedCustomMetrics)
                    .build())
            .build(), MetricMsg.class);
    
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MetricMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    networkConnection.send(driverId, msg);
  }
}
