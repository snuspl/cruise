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

import edu.snu.cay.common.aggregation.slave.AggregationSlave;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The aggregation slave task that runs on all Workers.
 * Sends aggregation message to aggregation master(driver).
 */
@TaskSide
public final class AggregationSlaveTask implements Task {
  private static final Logger LOG = Logger.getLogger(AggregationSlaveTask.class.getName());

  private final AggregationSlave aggregationSlave;
  private final Codec<String> codec;
  private final String taskId;

  @Inject
  private AggregationSlaveTask(final AggregationSlave aggregationSlave,
                               final SerializableCodec<String> codec,
                               @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId) {
    this.aggregationSlave = aggregationSlave;
    this.codec = codec;
    this.taskId = taskId;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);
    aggregationSlave.send(AggregationSlaveTask.class.getName(), codec.encode(taskId));
    return null;
  }
}
