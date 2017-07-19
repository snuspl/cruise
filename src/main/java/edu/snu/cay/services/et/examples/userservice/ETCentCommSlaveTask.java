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
package edu.snu.cay.services.et.examples.userservice;

import edu.snu.cay.common.centcomm.slave.SlaveSideCentCommMsgSender;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.userservice.ETCentCommExample.CENT_COMM_CLIENT_ID;

/**
 * The CentComm slave task that runs on all executors.
 * Sends a message to master(driver) and waits for a reply message.
 */
@TaskSide
final class ETCentCommSlaveTask implements Task {
  private static final Logger LOG = Logger.getLogger(ETCentCommSlaveTask.class.getName());

  private final SlaveSideCentCommMsgSender centCommMsgSender;
  private final Codec<String> codec;
  private final String taskId;
  private final EvalSideMsgHandler msgHandler;

  @Inject
  private ETCentCommSlaveTask(final SlaveSideCentCommMsgSender centCommMsgSender,
                              final SerializableCodec<String> codec,
                              @Parameter(TaskConfigurationOptions.Identifier.class) final String taskId,
                              final EvalSideMsgHandler msgHandler) {
    this.centCommMsgSender = centCommMsgSender;
    this.codec = codec;
    this.taskId = taskId;
    this.msgHandler = msgHandler;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);
    centCommMsgSender.send(CENT_COMM_CLIENT_ID, codec.encode(taskId));
    LOG.log(Level.INFO, "A message was sent. Waiting for response from the driver");
    msgHandler.waitForMessage();
    return null;
  }
}
