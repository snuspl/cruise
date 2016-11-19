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
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Network event handler implementation.
 */
@Private
final class NetworkEventHandler implements EventHandler<Message<String>> {
  private static final Logger LOG = Logger.getLogger(NetworkEventHandler.class.getName());

  @Inject
  private NetworkEventHandler() {
  }

  @Override
  public void onNext(final Message<String> stringMessage) {
    for (final String s : stringMessage.getData()) {
      LOG.log(Level.INFO, "Got message: {0}", s);
    }
  }
}
