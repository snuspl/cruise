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
package edu.snu.spl.cruise.services.et.evaluator.api;

import edu.snu.spl.cruise.services.et.avro.*;
import edu.snu.spl.cruise.services.et.evaluator.impl.MessageSenderImpl;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Interface for executors to send messages to the master and other executors.
 */
@EvaluatorSide
@DefaultImplementation(MessageSenderImpl.class)
public interface MessageSender {

  /**
   * Sends a ETMsg to the executor specified with {@code destId}.
   * This method is useful when especially resending failed messages, which are already in a form of {@link ETMsg}.
   */
  void sendMsg(String destId, ETMsg etMsg) throws NetworkException;

  /**
   * Sends a TableAccessReqMsg that requests the executor specified with {@code destId} to
   * process a remote access, parceling request metadata into the message.
   * Since the request can be transmitted multiple times through the multiple executors,
   * the message retains {@code origId}, an id of the executor where the operation is generated at the beginning.
   * If {@code replyRequired} is True, the target executor will send the result through {@link #sendTableAccessResMsg}.
   * The operation should be given a unique {@code opId}.
   */
  void sendTableAccessReqMsg(String origId, String destId, long opId,
                             String tableId, OpType opType, boolean replyRequired,
                             DataKey dataKey, @Nullable DataValue dataValue) throws NetworkException;

  /**
   * Sends a TableAccessReqMsg of the multi-key version.
   */
  void sendTableAccessReqMsg(String origId, String destId, long opId,
                             String tableId, OpType opType, boolean replyRequired,
                             DataKeys dataKeys, @Nullable DataValues dataValues) throws NetworkException;

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation.
   * The operation should be given a unique {@code opId}.
   */
  void sendTableAccessResMsg(String destId, long opId, String tableId,
                             @Nullable DataValue dataValue, boolean isSuccess) throws NetworkException;

  /**
   * Sends a TableAccessResMsg of the multi-key version.
   */
  void sendTableAccessResMsg(String destId, long opId, String tableId, DataKeys dataKeys,
                             DataValues dataValues, boolean isSuccess) throws NetworkException;

  /**
   * Sends a TableInitAckMsg that responds to TableInitMsg.
   */
  void sendTableInitAckMsg(long opId, String tableId) throws NetworkException;

  /**
   * Sends a TableLoadAckMsg that responds to TableLoadMsg.
   */
  void sendTableLoadAckMsg(long opId, String tableId) throws NetworkException;

  /**
   * Sends a TableDropAckMsg that responds to TableDropMsg.
   */
  void sendTableDropAckMsg(long opId, String tableId) throws NetworkException;

  /**
   * Sends a ChkpDoneMsg that responds to ChkpStartMsg.
   */
  void sendChkpDoneMsg(String chkpId, List<Integer> blockIds) throws NetworkException;

  /**
   * Sends a ChkpCommitMsg that notify master that the checkpoint has been moved to final location.
   */
  void sendChkpCommitMsg(String chkpId) throws NetworkException;

  /**
   * Sends a ChkpLoadDoneMsg that responds to ChkpLoadMsg.
   */
  void sendChkpLoadDoneMsg(String chkpId) throws NetworkException;

  /**
   * Sends a OwnershipReqMsg that requests up-to-date owner of a block.
   */
  void sendOwnershipReqMsg(String tableId, int blockId) throws NetworkException;

  /**
   * Sends a OwnershipSyncAckMsg that responds to OwnershipSyncMsg.
   */
  void sendOwnershipSyncAckMsg(long opId, String tableId, String deletedExecutorId) throws NetworkException;

  /**
   * Sends a message transferring ownership of a block from sender to receiver of migration.
   * The operation should be given a unique {@code opId}.
   */
  void sendOwnershipMsg(long opId, String tableId, int blockId,
                        String oldOwnerId, String newOwnerId) throws NetworkException;

  /**
   * Sends a response message for OwnershipMsg from receiver to sender of migration.
   * The operation should be given a unique {@code opId}.
   */
  void sendOwnershipAckMsg(long opId, String tableId, int blockId,
                           String oldOwnerId, String newOwnerId) throws NetworkException;

  /**
   * Sends a message to master to notify that the ownership of a block has been migrated successfully.
   * The operation should be given a unique {@code opId}.
   */
  void sendOwnershipMovedMsg(long opId, String tableId, int blockId) throws NetworkException;

  /**
   * Sends a message transferring data of a block from sender to receiver of migration.
   * The operation should be given a unique {@code opId}.
   */
  void sendDataMsg(long opId, String tableId, int blockId,
                   byte[] kvPairs, int numItems, int numTotalItems,
                   String senderId, String receiverId) throws NetworkException;

  /**
   * Sends a response message for DataMsg from receiver to sender of migration.
   * The operation should be given a unique {@code opId}.
   */
  void sendDataAckMsg(long opId, String tableId, int blockId,
                      String senderId, String receiverId) throws NetworkException;
  /**
   * Sends a message to master to notify that the data of a block has been migrated successfully.
   * The operation should be given a unique {@code opId}.
   */
  void sendDataMovedMsg(long opId, String tableId, int blockId, boolean moveOwnershipTogether) throws NetworkException;

  /**
   * Sends a message to master, containing the collected metrics in the Executor.
   */
  void sendMetricReportMsg(Map<String, Integer> tableToNumBlocks,
                           Map<String, Long> bytesReceivedGetResp,
                           Map<String, Integer> countSentGetReq,
                           List<ByteBuffer> encodedCustomMetrics) throws NetworkException;

  /**
   * Sends a tasklet status message to master.
   */
  void sendTaskletStatusMsg(String taskletId, TaskletStatusType status);
}
