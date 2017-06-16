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

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitInfoSerializer;
import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A message sender implementation.
 */
@DriverSide
public final class MessageSenderImpl implements MessageSender {
  private final NetworkConnection<ETMsg> networkConnection;
  private final ConfigurationSerializer confSerializer;

  @Inject
  private MessageSenderImpl(final NetworkConnection<ETMsg> networkConnection,
                            final ConfigurationSerializer confSerializer) {
    this.networkConnection = networkConnection;
    this.confSerializer = confSerializer;
  }

  @Override
  public void sendTableInitMsg(final long opId, final String executorId,
                               final TableConfiguration tableConf,
                               final List<String> blockOwnerList) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableInitMsg)
            .setOperationId(opId)
            .setTableInitMsg(
                TableInitMsg.newBuilder()
                    .setTableConf(confSerializer.toString(tableConf.getConfiguration()))
                    .setBlockOwners(blockOwnerList)
                    .build())
            .build(), TableControlMsg.class);
    
    final ETMsg msg = ETMsg.newBuilder()
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .setType(ETMsgType.TableControlMsg)
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableInit message", e);
    }
  }

  @Override
  public void sendTableLoadMsg(final long opId, final String executorId,
                               final String tableId,
                               final HdfsSplitInfo hdfsSplitInfo) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableLoadMsg)
            .setOperationId(opId)
            .setTableLoadMsg(
                TableLoadMsg.newBuilder()
                    .setTableId(tableId)
                    .setFileSplit(HdfsSplitInfoSerializer.serialize(hdfsSplitInfo))
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableLoad message", e);
    }
  }

  @Override
  public void sendTableDropMsg(final long opId, final String executorId, final String tableId) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.TableDropMsg)
            .setOperationId(opId)
            .setTableDropMsg(
                TableDropMsg.newBuilder()
                    .setTableId(tableId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableDrop message", e);
    }
  }

  @Override
  public void sendOwnershipUpdateMsg(final String executorId,
                                     final String tableId, final int blockId,
                                     final String oldOwnerId, final String newOwnerId) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.OwnershipUpdateMsg)
            .setOwnershipUpdateMsg(
                OwnershipUpdateMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockId(blockId)
                    .setOldOwnerId(oldOwnerId)
                    .setNewOwnerId(newOwnerId)
                    .build())
            .build(), TableControlMsg.class);
    
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending OwnershipUpdate message", e);
    }

  }

  @Override
  public void sendOwnershipSyncMsg(final long opId, final String executorId,
                                   final String tableId, final String deletedExecutorId) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableControlMsg.newBuilder()
            .setType(TableControlMsgType.OwnershipSyncMsg)
            .setOperationId(opId)
            .setOwnershipSyncMsg(
                OwnershipSyncMsg.newBuilder()
                    .setTableId(tableId)
                    .setDeletedExecutorId(deletedExecutorId)
                    .build())
            .build(), TableControlMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending OwnershipSync message", e);
    }
  }

  @Override
  public void sendMoveInitMsg(final long opId, final String tableId, final List<Integer> blockIds,
                              final String senderId, final String receiverId) {
    final byte[] innerMsg = AvroUtils.toBytes(
        MigrationMsg.newBuilder()
            .setOperationId(opId)
            .setType(MigrationMsgType.MoveInitMsg)
            .setMoveInitMsg(
                MoveInitMsg.newBuilder()
                    .setTableId(tableId)
                    .setBlockIds(blockIds)
                    .setSenderId(senderId)
                    .setReceiverId(receiverId)
                    .build())
            .build(), MigrationMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(senderId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending MoveInit message", e);
    }
  }

  @Override
  public void sendMetricStartMsg(final String executorId, final String serializedMetricConf) {
    final byte[] innerMsg = AvroUtils.toBytes(
        MetricMsg.newBuilder()
            .setType(MetricMsgType.MetricControlMsg)
            .setMetricControlMsg(
                MetricControlMsg.newBuilder()
                    .setType(MetricControlType.Start)
                    .setSerializedMetricConf(serializedMetricConf)
                    .build())
            .build(), MetricMsg.class);
    
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MetricMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg)).build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending MetricStart message", e);
    }
  }

  @Override
  public void sendMetricStopMsg(final String executorId) {
    final byte[] innerMsg = AvroUtils.toBytes(
        MetricMsg.newBuilder()
            .setType(MetricMsgType.MetricControlMsg)
            .setMetricControlMsg(
                MetricControlMsg.newBuilder()
                    .setType(MetricControlType.Stop)
                    .build())
            .build(), MetricMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MetricMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending MetricStop message", e);
    }
  }

  @Override
  public void sendTableAccessReqMsg(final String destId, final long opId, final TableAccessReqMsg tableAccessReqMsg)
      throws NetworkException {
    final byte[] innerMsg = AvroUtils.toBytes(
        TableAccessMsg.newBuilder()
            .setType(TableAccessMsgType.TableAccessReqMsg)
            .setOperationId(opId)
            .setTableAccessReqMsg(tableAccessReqMsg)
            .build(), TableAccessMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg))
        .build();
    
    try {
      networkConnection.send(destId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableAccessReq message", e);
    }
  }
}
