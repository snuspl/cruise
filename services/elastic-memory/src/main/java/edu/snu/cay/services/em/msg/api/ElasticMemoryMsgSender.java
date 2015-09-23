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

import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.msg.impl.ElasticMemoryMsgSenderImpl;
import org.apache.commons.lang.math.LongRange;
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
   * Send a CtrlMsg that tells the Evaluator specified with {@code destId} to
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
   * Send a CtrlMsg that tells the Evaluator specified with {@code destId} to
   * send {@code unitNum} units of its {@code dataType} data to the Evaluator specified with
   * {@code targetEvalId}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendCtrlMsg(final String destId,
                   final String dataType,
                   final String targetEvalId,
                   final int unitNum,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Send a DataMsg containing {@code unitIdPairList} to the Evaluator
   * named {@code destId}, specified by the type {@code dataType}.
   * The operation should be given a unique {@code operationId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(final String destId,
                   final String dataType,
                   final List<UnitIdPair> unitIdPairList,
                   final String operationId,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Send a ResultMsg to report to the Driver that the operation identified
   * by {@code operationId} has finished, with a {@code success} result.
   * The {@code dataType} and {@code idRangeSet} that was migrated is sent
   * together for the Driver to check and cleanup the operation.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendResultMsg(final boolean success,
                     final String dataType,
                     final Set<LongRange> idRangeSet,
                     final String operationId,
                     @Nullable final TraceInfo parentTraceInfo);

  /**
   * Send a RegisMsg to the Driver to register a partition, starting with
   * {@code unitStartId} and ending with {@code unitEndId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendRegisMsg(final String dataType,
                    final long unitStartId,
                    final long unitEndId,
                    @Nullable final TraceInfo parentTraceInfo);
}
