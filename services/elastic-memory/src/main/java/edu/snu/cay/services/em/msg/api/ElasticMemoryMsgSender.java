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

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.avro.UpdateResult;
import edu.snu.cay.services.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.util.Optional;
import org.htrace.TraceInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Interface for sending AvroElasticMemoryMessages to the driver and evaluators.
 */
@DefaultImplementation(ElasticMemoryMsgSenderImpl.class)
public interface ElasticMemoryMsgSender {

  /**
   * Sends a RemoteOpMsg that requests the Evaluator specified with {@code destId} to
   * process a data operation, parceling operation metadata into the message.
   * Since the operation can be transmitted multiple times across the multiple evaluators,
   * the message retains {@code origId}, an id of the Evaluator where the operation is generated at the beginning.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpMsg(final String origId,
                       final String destId,
                       final DataOpType operationType,
                       final String dataType,
                       final List<LongRange> dataKeyRanges,
                       final List<UnitIdPair> dataKVPairList,
                       final String operationId,
                       @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RemoteOpResultMsg that contains the result of the data operation specified with {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRemoteOpResultMsg(final String destId,
                             final List<UnitIdPair> dataKVPairList,
                             final List<LongRange> failedRanges,
                             final String operationId,
                             @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a CtrlMsg that tells the Evaluator specified with {@code destId} to
   * send its {@code dataType} data to the Evaluator specified with
   * {@code targetEvalId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendCtrlMsg(final String destId,
                   final String dataType,
                   final String targetEvalId,
                   final Set<LongRange> idRangeSet,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a CtrlMsg that tells the Evaluator specified with {@code destId} to
   * send {@code numUnits} units of its {@code dataType} data to the Evaluator specified with
   * {@code targetEvalId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendCtrlMsg(final String destId,
                   final String dataType,
                   final String targetEvalId,
                   final int numUnits,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

   /**
   * Sends a CtrlMsg to initiate moving data to the source Evaluator.
   * @param destId id of the Evaluator that receives this message
    *              (i.e., source Evaluator in terms of the data)
   * @param dataType type of the data to move
   * @param targetEvalId id of the Evaluator that receives the data
   * @param blocks block ids to move
   * @param operationId id associated with this operation
   * @param parentTraceInfo Trace information for HTrace
   */
  void sendCtrlMsg(final String destId,
                   final String dataType,
                   final String targetEvalId,
                   final List<Integer> blocks,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a DataMsg containing {@code unitIdPairList} to the Evaluator
   * named {@code destId}, specified by the type {@code dataType}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(final String destId,
                   final String dataType,
                   final List<UnitIdPair> unitIdPairList,
                   final int blockId,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a DataAckMsg to report to the Driver the success of data transfer.
   * Since the actual range or the number of units might differ from the user's request,
   * ({@code idRangeSet} is sent for the information.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataAckMsg(final Set<LongRange> idRangeSet,
                      final String operationId,
                      @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a RegisMsg to the Driver to register a partition, starting with
   * {@code unitStartId} and ending with {@code unitEndId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRegisMsg(final String dataType,
                    final long unitStartId,
                    final long unitEndId,
                    @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends an UpdateMsg to update the Evaluators' MemoryStore.
   */
  void sendUpdateMsg(final String destId,
                     final String operationId,
                     @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a UpdateAckMsg to notify the update result.
   */
  void sendUpdateAckMsg(final String operationId,
                        final UpdateResult result,
                        @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a request to update ownership for the given block.
   * @param destId Specifies the destination. The recipient is Driver when this field is empty.
   */
  void sendOwnershipMsg(final Optional<String> destId,
                        final String operationId,
                        final String dataType,
                        final int blockId,
                        final int oldOwnerId,
                        final int newOwnerId,
                        @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends an ACK message to Driver for notifying that the ownership has been updated successful.
   */
  void sendOwnershipAckMsg(final String operationId,
                           final String dataType,
                           final int blockId,
                           @Nullable final TraceInfo parentTraceInfo);

  /**
   * Sends a FailureMsg to notify the failure to the Driver.
   */
  void sendFailureMsg(final String operationId,
                      final String reason,
                      @Nullable final TraceInfo parentTraceInfo);
}
