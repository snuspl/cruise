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

import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.remote.transport.LinkListener;

import javax.inject.Inject;
import java.net.SocketAddress;

final class ShuffleTupleLinkListener<K, V> implements LinkListener<Message<Tuple<K, V>>> {

  private volatile LinkListener<Message<Tuple<K, V>>> tupleLinkListener;

  @Inject
  private ShuffleTupleLinkListener() {
  }

  public void setTupleLinkListener(final LinkListener<Message<Tuple<K, V>>> tupleLinkListener) {
    this.tupleLinkListener = tupleLinkListener;
  }

  @Override
  public void onSuccess(final Message<Tuple<K, V>> message) {
    if (tupleLinkListener == null) {
      throw new RuntimeException("The tuple link listener should be set first through TupleMessageSetup " +
          " before sending tuples.");
    }

    tupleLinkListener.onSuccess(message);
  }

  @Override
  public void onException(
      final Throwable throwable,
      final SocketAddress socketAddress,
      final Message<Tuple<K, V>> message) {
    if (tupleLinkListener == null) {
      throw new RuntimeException("The tuple link listener should be set first through TupleMessageSetup " +
          "before sending tuples.");
    }

    tupleLinkListener.onException(throwable, socketAddress, message);
  }
}
