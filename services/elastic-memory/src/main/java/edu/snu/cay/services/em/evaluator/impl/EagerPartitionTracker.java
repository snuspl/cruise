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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.PartitionTracker;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;
import org.apache.reef.annotations.audience.EvaluatorSide;

import javax.inject.Inject;

/**
 * Sends a partition register message to the driver as soon as it is called.
 */
@EvaluatorSide
public final class EagerPartitionTracker implements PartitionTracker {
  private static final String REGISTER_PARTITION = "register";

  private final ElasticMemoryMsgSender sender;

  @Inject
  private EagerPartitionTracker(final ElasticMemoryMsgSender sender) {
    this.sender = sender;
  }

  @Override
  public void registerPartition(final String dataType, final long startId, final long endId) {
    try (final TraceScope traceScope = Trace.startSpan(REGISTER_PARTITION)) {
      sender.sendRegisMsg(dataType, startId, endId, TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }
}
