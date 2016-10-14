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
import edu.snu.cay.services.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.reef.util.Optional;
import org.htrace.TraceInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for sending EMMsgs to the driver and evaluators.
 */
@DefaultImplementation(ElasticMemoryMsgSenderImpl.class)
public interface ElasticMemoryMsgSender {

  /**
   * Sends a RemoteOpReqMsg that requests the Evaluator specified with {@code destId} to
   * process a remote operation, parceling operation metadata into the message.
   * Since the operation can be transmitted multiple times across the multiple evaluators,
   * the message retains {@code origId}, an id of the Evaluator where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpReqMsg(final String origId,
                          final String destId,
                          final DataOpType operationType,
                          final List<KeyRange> dataKeyRanges,
                          final List<KeyValuePair> dataKVPairList,
                          final String operationId,
                          @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpReqMsg that requests the Evaluator specified with {@code destId} to
   * process a data operation, parceling operation metadata into the message.
   * Since the operation can be transmitted multiple times across the multiple evaluators,
   * the message retains {@code origId}, an id of the Evaluator where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpReqMsg(final String origId, final String destId, final DataOpType operationType,
                          final DataKey dataKey, final DataValue dataValue, final String operationId,
                          @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpResultMsg(final String destId,
                             final List<KeyValuePair> dataKVPairList,
                             final List<KeyRange> failedRanges,
                             final String operationId,
                             @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpResultMsg(final String destId,
                             final DataValue dataValue,
                             final boolean isSuccess,
                             final String operationId,
                             @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RoutingTableInitReqMsg that tells the driver to reply with the up-to-date global routing table.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRoutingTableInitReqMsg(@Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RoutingTableInitMsg that contains the up-to-date global routing table {@code blockLocations}.
   * It is always sent by Driver to the evaluator {@code destId} as a response for RoutingTableInitReqMsg.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRoutingTableInitMsg(final String destId,
                               final List<Integer> blockLocations,
                               @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RoutingTableUpdateMsg that contains recently updated block information by EM.move().
   * It is for Driver to tell evaluator {@code destId} that
   * {@code blocks} are moved from {@code oldEvalId} to {@code newEvalId}.
   */
  void sendRoutingTableUpdateMsg(final String destId, final List<Integer> blocks,
                                 final String oldEvalId, final String newEvalId,
                                 @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a MoveInitMsg to initiate moving data blocks to the source Evaluator.
   * @param destId id of the Evaluator that receives this message
   *              (i.e., source Evaluator in terms of the data)
   * @param receiverId id of the Evaluator that receives the data
   * @param blocks block ids to move
   * @param operationId id associated with this operation
   * @param parentTraceInfo Trace information for HTrace
   */
  void sendMoveInitMsg(final String destId,
                       final String receiverId,
                       final List<Integer> blocks,
                       final String operationId,
                       @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a DataMsg containing list of {@code keyValuePairs} to the Evaluator named {@code destId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(final String destId,
                   final List<KeyValuePair> keyValuePairs,
                   final int blockId,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a response message for DataMsg to the Evaluator named {@code destId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataAckMsg(final String destId,
                      final int blockId,
                      final String operationId,
                      @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a request to update ownership for the given block.
   * @param destId Specifies the destination. The recipient is Driver when this field is empty.
   */
  void sendOwnershipMsg(final Optional<String> destId,
                        final String senderId,
                        final String operationId,
                        final int blockId,
                        final int oldOwnerId,
                        final int newOwnerId,
                        @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a response message for OwnershipMsg.
   * @param destId Specifies the destination. The recipient is Driver when this field is empty.
   */
  void sendOwnershipAckMsg(final Optional<String> destId,
                           final String operationId,
                           final int blockId,
                           final int oldOwnerId,
                           final int newOwnerId,
                           @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a BlockMoved message to driver for notifying that the moving a block is completed.
   */
  void sendBlockMovedMsg(final String operationId,
                         final int blockId,
                         @Nullable final TraceInfo parentTraceInfo);

  /**
   * TODO #90: handle failures during move
   * Sends a FailureMsg to notify the failure to the Driver.
   */
  void sendFailureMsg(final String operationId,
                      final String reason,
                      @Nullable final TraceInfo parentTraceInfo);
}
