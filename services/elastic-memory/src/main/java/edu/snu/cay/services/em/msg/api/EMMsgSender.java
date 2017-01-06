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
package edu.snu.cay.services.em.msg.api;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.impl.EMMsgSenderImpl;
import org.apache.reef.util.Optional;
import org.htrace.TraceInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for sending EMMsgs to the driver and evaluators.
 */
@DefaultImplementation(EMMsgSenderImpl.class)
public interface EMMsgSender {

  /**
   * Sends a RemoteOpReqMsg that requests the Evaluator specified with {@code destId} to
   * process a remote operation, parceling operation metadata into the message.
   * Since the operation can be transmitted multiple times across the multiple evaluators,
   * the message retains {@code origId}, an id of the Evaluator where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpReqMsg(String origId,
                          String destId,
                          DataOpType operationType,
                          List<KeyRange> dataKeyRanges,
                          List<KeyValuePair> dataKVPairList,
                          String operationId,
                          @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpReqMsg that requests the Evaluator specified with {@code destId} to
   * process a data operation, parceling operation metadata into the message.
   * Since the operation can be transmitted multiple times across the multiple evaluators,
   * the message retains {@code origId}, an id of the Evaluator where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpReqMsg(String origId, String destId, DataOpType operationType,
                          DataKey dataKey, DataValue dataValue, String operationId,
                          @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpResultMsg(String destId,
                             List<KeyValuePair> dataKVPairList,
                             List<KeyRange> failedRanges,
                             String operationId,
                             @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpResultMsg(String destId,
                             DataValue dataValue,
                             boolean isSuccess,
                             String operationId,
                             @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a OwnershipCacheInitReqMsg that tells the driver to reply with the up-to-date global ownership info.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendOwnershipCacheInitReqMsg(@Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a OwnershipCacheInitMsg that contains the up-to-date global ownership info {@code blockLocations}.
   * It is always sent by Driver to the evaluator {@code destId} as a response for OwnershipCacheInitReqMsg.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendOwnershipCacheInitMsg(String destId,
                                 List<Integer> blockLocations,
                                 @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a OwnershipCacheUpdateMsg that contains recently updated block information by EM.move().
   * It is for Driver to tell evaluator {@code destId} that
   * {@code blocks} are moved from {@code oldEvalId} to {@code newEvalId}.
   */
  void sendOwnershipCacheUpdateMsg(String destId, List<Integer> blocks,
                                   String oldEvalId, String newEvalId,
                                   @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a MoveInitMsg to initiate moving data blocks to the source Evaluator.
   * @param destId id of the Evaluator that receives this message
   *              (i.e., source Evaluator in terms of the data)
   * @param receiverId id of the Evaluator that receives the data
   * @param blocks block ids to move
   * @param operationId id associated with this operation
   * @param parentTraceInfo Trace information for HTrace
   */
  void sendMoveInitMsg(String destId,
                       String receiverId,
                       List<Integer> blocks,
                       String operationId,
                       @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a DataMsg containing list of {@code keyValuePairs} to the Evaluator named {@code destId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(String destId,
                   List<KeyValuePair> keyValuePairs,
                   int blockId,
                   String operationId,
                   @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a response message for DataMsg to the Evaluator named {@code destId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataAckMsg(String destId,
                      int blockId,
                      String operationId,
                      @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a request to update ownership for the given block.
   * @param destId Specifies the destination. The recipient is Driver when this field is empty.
   */
  void sendOwnershipMsg(Optional<String> destId,
                        String senderId,
                        String operationId,
                        int blockId,
                        int oldOwnerId,
                        int newOwnerId,
                        @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a response message for OwnershipMsg.
   * @param destId Specifies the destination. The recipient is Driver when this field is empty.
   */
  void sendOwnershipAckMsg(Optional<String> destId,
                           String operationId,
                           int blockId,
                           int oldOwnerId,
                           int newOwnerId,
                           @Nullable TraceInfo parentTraceInfo);

  /**
   * Sends a BlockMoved message to driver for notifying that the moving a block is completed.
   */
  void sendBlockMovedMsg(String operationId,
                         int blockId,
                         @Nullable TraceInfo parentTraceInfo);

  /**
   * TODO #90: handle failures during move
   * Sends a FailureMsg to notify the failure to the Driver.
   */
  void sendFailureMsg(String operationId,
                      String reason,
                      @Nullable TraceInfo parentTraceInfo);
}
