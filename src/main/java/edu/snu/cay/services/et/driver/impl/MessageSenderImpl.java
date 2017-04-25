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
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.annotation.Nullable;
import javax.inject.Inject;
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
                               final List<String> blockOwnerList,
                               @Nullable final HdfsSplitInfo fileSplit) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableInitMsg)
                .setOperationId(opId)
                .setTableInitMsg(
                    TableInitMsg.newBuilder()
                        .setTableConf(confSerializer.toString(tableConf.getConfiguration()))
                        .setBlockOwners(blockOwnerList)
                        .setFileSplit(fileSplit == null ? null : HdfsSplitInfoSerializer.serialize(fileSplit))
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableInit message", e);
    }
  }

  @Override
  public void sendTableDropMsg(final long opId, final String executorId, final String tableId) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableDropMsg)
                .setOperationId(opId)
                .setTableDropMsg(
                    TableDropMsg.newBuilder()
                        .setTableId(tableId)
                        .build()
                ).build()
        ).build();

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
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.OwnershipUpdateMsg)
                .setOwnershipUpdateMsg(
                    OwnershipUpdateMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockId(blockId)
                        .setOldOwnerId(oldOwnerId)
                        .setNewOwnerId(newOwnerId)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending OwnershipUpdate message", e);
    }

  }

  @Override
  public void sendOwnershipSyncMsg(final long opId, final String executorId,
                                   final String tableId, final String deletedExecutorId) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.OwnershipSyncMsg)
                .setOperationId(opId)
                .setOwnershipSyncMsg(
                    OwnershipSyncMsg.newBuilder()
                        .setTableId(tableId)
                        .setDeletedExecutorId(deletedExecutorId)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending OwnershipSync message", e);
    }
  }

  @Override
  public void sendMoveInitMsg(final long opId, final String tableId, final List<Integer> blockIds,
                              final String senderId, final String receiverId) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.MigrationMsg)
        .setMigrationMsg(
            MigrationMsg.newBuilder()
                .setOperationId(opId)
                .setType(MigrationMsgType.MoveInitMsg)
                .setMoveInitMsg(
                    MoveInitMsg.newBuilder()
                        .setTableId(tableId)
                        .setBlockIds(blockIds)
                        .setSenderId(senderId)
                        .setReceiverId(receiverId)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(senderId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending MoveInit message", e);
    }
  }
}
