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
  public void sendTableAccessReqMsg(final String origId, final String destId,
                                    final long opId, final String tableId,
                                    final OpType opType, final boolean replyRequired,
                                    final DataKey dataKey, @Nullable final DataValue dataValue)
      throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setTableAccessMsg(
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
                        .build()
                ).build()
        ).build();

    networkConnection.send(destId, msg);
  }

  @Override
  public void sendTableAccessResMsg(final String destId, final long opId, final String tableId,
                                    @Nullable final DataValue dataValue, final boolean isSuccess)
      throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setTableAccessMsg(
            TableAccessMsg.newBuilder()
                .setType(TableAccessMsgType.TableAccessResMsg)
                .setOperationId(opId)
                .setTableAccessResMsg(
                    TableAccessResMsg.newBuilder()
                        .setIsSuccess(isSuccess)
                        .setTableId(tableId)
                        .setDataValue(dataValue)
                        .build()
                ).build()
        ).build();

    networkConnection.send(destId, msg);
  }

  @Override
  public void sendTableInitAckMsg(final long opId, final String tableId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableInitAckMsg)
                .setOperationId(opId)
                .setTableInitAckMsg(
                    TableInitAckMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setTableId(tableId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendTableDropAckMsg(final long opId, final String tableId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableDropAckMsg)
                .setOperationId(opId)
                .setTableDropAckMsg(
                    TableDropAckMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setTableId(tableId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendOwnershipSyncAckMsg(final long opId, final String tableId,
                                      final String deletedExecutorId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.OwnershipSyncAckMsg)
                .setOperationId(opId)
                .setOwnershipSyncAckMsg(
                    OwnershipSyncAckMsg.newBuilder()
                        .setTableId(tableId)
                        .setDeletedExecutorId(executorId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendOwnershipMsg(final long opId, final String tableId, final int blockId,
                               final String oldOwnerId, final String newOwnerId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.OwnershipMsg)
                .setOwnershipMsg(
                    OwnershipMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .setOldOwnerId(oldOwnerId)
                        .setNewOwnerId(newOwnerId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(newOwnerId, msg);
  }

  @Override
  public void sendOwnershipAckMsg(final long opId, final String tableId, final int blockId,
                                  final String oldOwnerId, final String newOwnerId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.OwnershipAckMsg)
                .setOwnershipAckMsg(
                    OwnershipAckMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .setOldOwnerId(oldOwnerId)
                        .setNewOwnerId(newOwnerId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(oldOwnerId, msg);
  }

  @Override
  public void sendOwnershipMovedMsg(final long opId, final String tableId, final int blockId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.OwnershipMovedMsg)
                .setOwnershipMovedMsg(
                    OwnershipMovedMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendDataMsg(final long opId, final String tableId, final int blockId,
                          final List<KVPair> kvPairs,
                          final String senderId, final String receiverId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.DataMsg)
                .setDataMsg(
                    DataMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .setKvPairs(kvPairs)
                        .setSenderId(senderId)
                        .setReceiverId(receiverId)
                        .build()
                ).build()
        ).build();

    networkConnection.send(receiverId, msg);
  }

  @Override
  public void sendDataAckMsg(final long opId, final String tableId, final int blockId,
                             final String senderId, final String receiverId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.DataAckMsg)
                .setDataAckMsg(
                    DataAckMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .setSenderId(senderId)
                        .setReceiverId(receiverId)
                        .build()
                )
                .build())
        .build();

    networkConnection.send(senderId, msg);
  }

  @Override
  public void sendDataMovedMsg(final long opId, final String tableId, final int blockId) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.DataMovedMsg)
                .setDataMovedMsg(
                    DataMovedMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .build()
                )
                .build()
        )
        .build();

    networkConnection.send(driverId, msg);
  }

  @Override
  public void sendMetricMsg(final Map<String, Integer> tableToNumBlocks,
                            final Map<String, Long> bytesReceivedGetResp,
                            final Map<String, Integer> countSentGetReq,
                            final List<ByteBuffer> encodedCustomMetrics) throws NetworkException {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MetricMsg)
        .setMetricMsg(
            MetricMsg.newBuilder()
                .setTableToNumBlocks(tableToNumBlocks)
                .setBytesReceivedGetResp(bytesReceivedGetResp)
                .setCountSentGetReq(countSentGetReq)
                .setHostname(HostnameResolver.resolve())
                .setCustomMetrics(encodedCustomMetrics)
                .build()
        )
        .build();
    networkConnection.send(driverId, msg);
  }
}
