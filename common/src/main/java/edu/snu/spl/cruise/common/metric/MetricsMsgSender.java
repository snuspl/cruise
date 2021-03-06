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
package edu.snu.spl.cruise.common.metric;

import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * Sends MetricsMsg that consists of raw metrics and additional information
 * such as iteration number.
 * @param <T> Type of message specified by client of MetricsCollectionService.
 */
@EvaluatorSide
public interface MetricsMsgSender<T> {

  /**
   * Sends the message.
   * @param message The message containing additional information that
   *                client wants to send together with raw metrics.
   */
  void send(T message);
}
