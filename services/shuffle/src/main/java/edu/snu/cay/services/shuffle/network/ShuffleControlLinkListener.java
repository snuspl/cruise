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
package edu.snu.cay.services.shuffle.network;

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;

final class ShuffleControlLinkListener implements LinkListener<Message<ShuffleControlMessage>> {

  private volatile LinkListener<Message<ShuffleControlMessage>> controlLinkListener;

  @Inject
  private ShuffleControlLinkListener() {
  }

  void setControlLinkListener(final LinkListener<Message<ShuffleControlMessage>> controlLinkListener) {
    this.controlLinkListener = controlLinkListener;
  }

  @Override
  public void onSuccess(final Message<ShuffleControlMessage> message) {
    if (controlLinkListener == null) {
      throw new RuntimeException("The control link listener should be set first through ControlMessageSetup " +
          "before sending messages.");
    }

    controlLinkListener.onSuccess(message);
  }

  @Override
  public void onException(
      final Throwable throwable,
      final SocketAddress socketAddress,
      final Message<ShuffleControlMessage> message) {
    if (controlLinkListener != null) {
      throw new RuntimeException("The control link listener should be set first through ControlMessageSetup " +
          "before sending messages.");
    }

    controlLinkListener.onException(throwable, socketAddress, message);
  }
}
