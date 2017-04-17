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
package edu.snu.cay.dolphin.async;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Dolphin-async specific parameters.
 */
public final class DolphinParameters {
  @NamedParameter(doc = "Maximum number of epochs to run before termination",
                  short_name = "max_num_epochs")
  public final class MaxNumEpochs implements Name<Integer> {
  }

  @NamedParameter(doc = "Mini-batch size in number of training data instances",
      short_name = "mini_batch_size")
  public final class MiniBatchSize implements Name<Integer> {
  }

  @NamedParameter(doc = "Number of threads to run Trainer with",
      short_name = "num_trainer_threads", default_value = "1")
  public final class NumTrainerThreads implements Name<Integer> {
  }

  @NamedParameter(doc = "Desired memory size for each worker evaluator (MBs)",
                  short_name = "worker_mem_size",
                  default_value = "128")
  public final class WorkerMemSize implements Name<Integer> {
  }

  @NamedParameter(doc = "Desired memory size for each server evaluator (MBs)",
                  short_name = "server_mem_size",
                  default_value = "128")
  public final class ServerMemSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of workers",
                  short_name = "num_workers",
                  default_value = "1")
  public final class NumWorkers implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of servers",
                  short_name = "number_servers",
                  default_value = "1")
  public final class NumServers implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of CPU cores for each worker evaluator",
                  short_name = "num_worker_cores",
                  default_value = "1")
  public final class NumWorkerCores implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of CPU cores for each server evaluator",
                  short_name = "num_server_cores",
                  default_value = "1")
  public final class NumServerCores implements Name<Integer> {
  }

  @NamedParameter(doc = "The period to flush server-side metrics (in millisecond)",
                  short_name = "server_metric_flush_period_ms")
  public final class ServerMetricFlushPeriodMs implements Name<Long> {
  }
}
