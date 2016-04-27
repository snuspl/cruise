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
package edu.snu.cay.services.ps.driver.impl;

import edu.snu.cay.services.ps.avro.AvroParameterServerMsg;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives the messages from ParameterServers and ParameterWorkers.
 * Currently used to exchange the routing table between
 * {@link edu.snu.cay.services.ps.server.partitioned.DynamicPartitionedParameterServer} and
 * {@link edu.snu.cay.services.ps.worker.partitioned.DynamicServerResolver}.
 */
public class PSMessageHandler implements EventHandler<Message<AvroParameterServerMsg>> {
  private static final Logger LOG = Logger.getLogger(PSMessageHandler.class.getName());

  @Override
  public void onNext(final Message<AvroParameterServerMsg> avroParameterServerMsgMessage) {
    LOG.log(Level.SEVERE, "Message has arrived. The content: {0}", avroParameterServerMsgMessage);
  }
}
