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
import org.apache.htrace.TraceInfo;
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
   * send its {@code dataClassName} data to the Evaluator specified with
   * {@code targetEvalId}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendCtrlMsg(final String destId,
                   final String dataClassName,
                   final String targetEvalId,
                   final Set<LongRange> idRangeSet,
                   @Nullable final TraceInfo parentTraceInfo);

  /**
   * Send a DataMsg containing {@code unitIdPairList} to the Evaluator
   * named {@code destId}. The data key is {@code dataClassName}.
   * Include {@code parentTraceInfo} to continue tracing this message.
   */
  void sendDataMsg(final String destId,
                   final String dataClassName,
                   final List<UnitIdPair> unitIdPairList,
                   @Nullable final TraceInfo parentTraceInfo);

  void sendRegisMsg(final String dataClassName,
                    final long unitStartId,
                    final long unitEndId,
                    @Nullable final TraceInfo parentTraceInfo);
}
