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
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

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
                                    final String operationId, final String tableId,
                                    final AccessType accessType,
                                    final DataKey dataKey, final DataValue dataValue) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setTableAccessMsg(
            TableAccessMsg.newBuilder()
                .setType(TableAccessMsgType.TableAccessReqMsg)
                .setOperationId(operationId)
                .setTableAccessReqMsg(
                    TableAccessReqMsg.newBuilder()
                        .setOrigId(origId)
                        .setTableId(tableId)
                        .setAccessType(accessType)
                        .setDataKey(dataKey)
                        .setDataValue(dataValue)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(destId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableAccessReq message", e);
    }
  }

  @Override
  public void sendTableAccessResMsg(final String destId, final String operationId,
                                    final DataValue dataValue, final boolean isSuccess) {
    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableAccessMsg)
        .setTableAccessMsg(
            TableAccessMsg.newBuilder()
                .setType(TableAccessMsgType.TableAccessResMsg)
                .setOperationId(operationId)
                .setTableAccessResMsg(
                    TableAccessResMsg.newBuilder()
                        .setIsSuccess(isSuccess)
                        .setDataValue(dataValue)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(destId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableAccessReq message", e);
    }
  }

  @Override
  public void sendTableInitAckMsg(final String tableId) {

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableInitAckMsg)
                .setTableInitAckMsg(
                    TableInitAckMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setTableId(tableId)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(driverId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableInitAck message", e);
    }
  }

  @Override
  public void sendOwnershipMsg(final String tableId, final int blockId,
                               final String oldOwnerId, final String newOwnerId) {

  }

  @Override
  public void sendOwnershipAckMsg(final String tableId, final int blockId,
                                  final String oldOwnerId, final String newOwnerId) {

  }

  @Override
  public void sendOwnershipMovedMsg(final String tableId, final int blockId) {

  }

  @Override
  public void sendDataMsg(final String tableId, final int blockId,
                          final List<KVPair> kvPairs,
                          final String senderId, final String receiverId) {

  }

  @Override
  public void sendDataAckMsg(final String tableId, final int blockId,
                             final String senderId, final String receiverId) {

  }

  @Override
  public void sendDataMovedMsg(final String tableId, final int blockId) {

  }
}
