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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.ETMsg;
import edu.snu.cay.services.et.avro.ETMsgType;
import edu.snu.cay.services.et.avro.TaskletMsg;
import edu.snu.cay.services.et.avro.TaskletMsgType;
import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.TaskletIdentifier;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;

/**
 * Created by xyzi on 28/11/2017.
 */
public final class TaskletCustomMsgSender {
  private final NetworkConnection<ETMsg> networkConnection;
  private final String taskletId;
  private final String driverId;

  @Inject
  private TaskletCustomMsgSender(@Parameter(TaskletIdentifier.class) final String taskletId,
                                 final NetworkConnection<ETMsg> networkConnection,
                                 @Parameter(DriverIdentifier.class) final String driverId) {
    this.networkConnection = networkConnection;
    this.taskletId = taskletId;
    this.driverId = driverId;
  }

  public void send(final byte[] message) {
    final byte[] innerMsg = AvroUtils.toBytes(
        TaskletMsg.newBuilder()
            .setType(TaskletMsgType.TaskletCustomMsg)
            .setTaskletId(taskletId)
            .setTaskletCustomMsg(ByteBuffer.wrap(message))
            .build(), TaskletMsg.class);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TaskletMsg)
        .setInnerMsg(ByteBuffer.wrap(innerMsg)).build();

    try {
      networkConnection.send(driverId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TaskletCustomMessage", e);
    }
  }
}
