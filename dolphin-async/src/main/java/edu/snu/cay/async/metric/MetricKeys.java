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
package edu.snu.cay.async.metric;

/**
 * Keys to identify metrics that come from Async-Dolphin.
 */
public final class MetricKeys {

  /**
   * Should not be instantiated.
   */
  private MetricKeys() {
  }

  // Keys to get/set the metrics in the worker.
  public static final String WORKER_COMPUTE_TIME =
      "METRIC_WORKER_COMPUTE_TIME";
}
