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
package edu.snu.spl.cruise.services.et.evaluator.impl;

import edu.snu.spl.cruise.services.et.evaluator.api.TaskletCustomMsgHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A default implementation of {@link TaskletCustomMsgHandler}.
 */
public final class DefaultTaskletCustomMsgHandler implements TaskletCustomMsgHandler {
  private static final Logger LOG = Logger.getLogger(DefaultTaskletCustomMsgHandler.class.getName());

  @Inject
  private DefaultTaskletCustomMsgHandler() {

  }

  @Override
  public void onNext(final byte[] bytes) {
    LOG.log(Level.FINE, "DefaultTaskletCustomMsgHandler received a msg.");
  }
}
