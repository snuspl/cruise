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

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network event handler implementation.
 */
@Private
final class NetworkLinkListener implements LinkListener<Message<String>> {
  private static final Logger LOG = Logger.getLogger(NetworkLinkListener.class.getName());

  @Inject
  private NetworkLinkListener() {
  }

  @Override
  public void onSuccess(final Message<String> stringMessage) {
    for (final String msg : stringMessage.getData()) {
      LOG.log(Level.INFO, "Success on sending message: {0}", msg);
    }
  }

  @Override
  public void onException(final Throwable throwable, final SocketAddress socketAddress,
                          final Message<String> stringMessage) {
    for (final String msg : stringMessage.getData()) {
      LOG.log(Level.WARNING, "Failure on sending message: " + msg + " to SockAddr: " + socketAddress, throwable);
    }
  }
}
