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
package edu.snu.cay.dolphin.async.core.worker;

import edu.snu.cay.dolphin.async.DolphinMsg;
import edu.snu.cay.dolphin.async.network.MessageHandler;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;

import javax.inject.Inject;

/**
 * A worker-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
@EvaluatorSide
public final class DummyMsgHandler implements MessageHandler {
  @Inject
  private DummyMsgHandler() {
  }

  @Override
  public synchronized void onNext(final Message<DolphinMsg> msg) {

  }
}
