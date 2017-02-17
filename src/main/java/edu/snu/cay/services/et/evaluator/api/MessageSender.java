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
package edu.snu.cay.services.et.evaluator.api;

import edu.snu.cay.services.et.avro.OpType;
import edu.snu.cay.services.et.avro.DataKey;
import edu.snu.cay.services.et.avro.DataValue;
import edu.snu.cay.services.et.avro.KVPair;
import edu.snu.cay.services.et.evaluator.impl.MessageSenderImpl;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for executors to send messages to the master and other executors.
 */
@EvaluatorSide
@DefaultImplementation(MessageSenderImpl.class)
public interface MessageSender {

  /**
   * Sends a TableAccessReqMsg that requests the executor specified with {@code destId} to
   * process a remote access, parceling request metadata into the message.
   * Since the request can be transmitted multiple times through the multiple executors,
   * the message retains {@code origId}, an id of the executor where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   */
  void sendTableAccessReqMsg(String origId, String destId, long operationId,
                             String tableId, OpType opType, DataKey dataKey, @Nullable DataValue dataValue);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   */
  void sendTableAccessResMsg(String destId, long operationId,
                             @Nullable DataValue dataValue, boolean isSuccess);

  /**
   * Sends a TableInitAckMsg that responds to TableInitMsg.
   */
  void sendTableInitAckMsg(String tableId);

  /**
   * Sends a message transferring ownership of a block from sender to receiver of migration.
   */
  void sendOwnershipMsg(String tableId, int blockId,
                        String oldOwnerId, String newOwnerId);

  /**
   * Sends a response message for OwnershipMsg from receiver to sender of migration.
   */
  void sendOwnershipAckMsg(String tableId, int blockId,
                           String oldOwnerId, String newOwnerId);

  /**
   * Sends a message to master to notify that the ownership of a block has been migrated successfully.
   */
  void sendOwnershipMovedMsg(String tableId, int blockId);

  /**
   * Sends a message transferring data of a block from sender to receiver of migration.
   */
  void sendDataMsg(String tableId, int blockId,
                   List<KVPair> kvPairs,
                   String senderId, String receiverId);

  /**
   * Sends a response message for DataMsg from receiver to sender of migration.
   */
  void sendDataAckMsg(String tableId, int blockId,
                      String senderId, String receiverId);
  /**
   * Sends a message to master to notify that the ownership of a block has been migrated successfully.
   */
  void sendDataMovedMsg(String tableId, int blockId);
}
