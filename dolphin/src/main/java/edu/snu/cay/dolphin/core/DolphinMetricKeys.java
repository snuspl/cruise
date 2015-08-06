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
package edu.snu.cay.dolphin.core;

/**
 * Keys to identify metrics that come from Dolphin.
 */
public class DolphinMetricKeys {

  // Keys to get/set the metrics in the ControllerTask.
  public static final String CONTROLLER_TASK_SEND_DATA_START =
      "METRIC_CONTROLLER_TASK_SEND_DATA_START";
  public static final String CONTROLLER_TASK_SEND_DATA_END =
      "METRIC_CONTROLLER_TASK_SEND_DATA_END";
  public static final String CONTROLLER_TASK_USER_CONTROLLER_TASK_START =
      "METRIC_CONTROLLER_TASK_USER_CONTROLLER_TASK_START";
  public static final String CONTROLLER_TASK_USER_CONTROLLER_TASK_END =
      "METRIC_CONTROLLER_TASK_USER_CONTROLLER_TASK_END";
  public static final String CONTROLLER_TASK_RECEIVE_DATA_START =
      "METRIC_CONTROLLER_TASK_RECEIVE_DATA_START";
  public static final String CONTROLLER_TASK_RECEIVE_DATA_END =
      "METRIC_CONTROLLER_TASK_RECEIVE_DATA_END";

  // Keys to get/set the metrics in the ComputeTask.
  public static final String COMPUTE_TASK_SEND_DATA_START =
      "METRIC_COMPUTE_TASK_SEND_DATA_START";
  public static final String COMPUTE_TASK_SEND_DATA_END =
      "METRIC_COMPUTE_TASK_SEND_DATA_END";
  public static final String COMPUTE_TASK_USER_COMPUTE_TASK_START =
      "METRIC_COMPUTE_TASK_USER_COMPUTE_TASK_START";
  public static final String COMPUTE_TASK_USER_COMPUTE_TASK_END =
      "METRIC_COMPUTE_TASK_USER_COMPUTE_TASK_END";
  public static final String COMPUTE_TASK_RECEIVE_DATA_START =
      "METRIC_COMPUTE_TASK_RECEIVE_DATA_START";
  public static final String COMPUTE_TASK_RECEIVE_DATA_END =
      "METRIC_COMPUTE_TASK_RECEIVE_DATA_END";
}
