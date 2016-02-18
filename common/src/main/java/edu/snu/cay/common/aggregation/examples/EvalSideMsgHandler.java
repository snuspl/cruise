/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.common.aggregation.examples;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Evaluator-side message handler.
 * Does nothing, but we need this class as a placeholder to use Aggregation Service.
 */
@EvaluatorSide
public final class EvalSideMsgHandler implements EventHandler<AggregationMessage> {

  @Inject
  private EvalSideMsgHandler() {
  }

  @Override
  public void onNext(final AggregationMessage message) {
    throw new RuntimeException("Evaluators are not intended to receive aggregation messages.");
  }
}
