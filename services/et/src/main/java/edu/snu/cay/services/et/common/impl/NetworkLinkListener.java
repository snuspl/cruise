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
package edu.snu.cay.services.et.common.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.evaluator.impl.RemoteAccessOpSender;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network link listener implementation.
 */
@Private
final class NetworkLinkListener implements LinkListener<Message<ETMsg>> {
  private static final Logger LOG = Logger.getLogger(NetworkLinkListener.class.getName());

  // TODO #209: OpSender is an executor-specific component for now
  private final RemoteAccessOpSender remoteAccessOpSender;

  @Inject
  private NetworkLinkListener(final RemoteAccessOpSender remoteAccessOpSender) {
    this.remoteAccessOpSender = remoteAccessOpSender;
  }

  @Override
  public void onSuccess(final Message<ETMsg> stringMessage) {
    for (final ETMsg msg : stringMessage.getData()) {
      LOG.log(Level.FINE, "Success on sending message: {0}", msg);
    }
  }

  @Override
  public void onException(final Throwable throwable, final SocketAddress socketAddress,
                          final Message<ETMsg> stringMessage) {
    for (final ETMsg msg : stringMessage.getData()) {
      // handle failed table access request msgs, which may fail by reconfiguration
      if (msg.getType().equals(ETMsgType.TableAccessMsg)) {
        final TableAccessMsg tableAccessMsg = AvroUtils.fromBytes(msg.getInnerMsg().array(), TableAccessMsg.class);
        if (tableAccessMsg.getType().equals(TableAccessMsgType.TableAccessReqMsg)) {
          final TableAccessReqMsg tableAccessReqMsg = tableAccessMsg.getTableAccessReqMsg();
          LOG.log(Level.INFO, String.format("Failure on sending TableAccessReqMsg. tableId: %s, targetAddr: %s",
              tableAccessReqMsg.getTableId(), socketAddress);

          remoteAccessOpSender.onFailedMsg(tableAccessReqMsg.getTableId(), tableAccessReqMsg.getDataKey(),
              tableAccessReqMsg.getReplyRequired(), tableAccessMsg.getOperationId(), msg);
          return;
        }
      }

      LOG.log(Level.WARNING, "Failure on sending message: " + msg + " to SockAddr: " + socketAddress, throwable);
    }
  }
}
