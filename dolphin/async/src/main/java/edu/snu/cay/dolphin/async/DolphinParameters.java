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
  @NamedParameter(doc = "Starting epoch index for each worker")
  public final class StartingEpochIdx implements Name<Integer> {
  }

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

  @NamedParameter(doc = "The queue size to handle remote access messages at server",
                  short_name = "server_handler_queue_size",
                  default_value = "500000")
  public final class ServerHandlerQueueSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The queue size to handle remote access messages at worker",
                  short_name = "worker_handler_queue_size",
                  default_value = "500000")
  public final class WorkerHandlerQueueSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The queue size to send remote access messages at server",
                  short_name = "server_sender_queue_size",
                  default_value = "500000")
  public final class ServerSenderQueueSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The queue size to send remote access messages at worker",
                  short_name = "worker_sender_queue_size",
                  default_value = "500000")
  public final class WorkerSenderQueueSize implements Name<Integer> {
  }

  @NamedParameter(doc = "The size of queue to handle remote access messages at server",
                  short_name = "num_server_handler_threads",
                  default_value = "2")
  public final class NumServerHandlerThreads implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of threads to handle remote access messages at worker",
                  short_name = "num_worker_handler_threads",
                  default_value = "2")
  public final class NumWorkerHandlerThreads implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of threads to send remote access messages at server",
                  short_name = "num_server_sender_threads",
                  default_value = "2")
  public final class NumServerSenderThreads implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of threads to send remote access messages at worker",
                  short_name = "num_worker_sender_threads",
                  default_value = "2")
  public final class NumWorkerSenderThreads implements Name<Integer> {
  }
  
  @NamedParameter(doc = "The path of test data",
                  short_name = "test_data_path",
                  default_value = TestDataPath.NONE)
  public final class TestDataPath implements Name<String> {
    public static final String NONE = "";
    private TestDataPath() {
    }
  }
}
