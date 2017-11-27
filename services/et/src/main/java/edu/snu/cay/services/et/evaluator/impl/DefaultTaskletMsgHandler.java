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

import edu.snu.cay.services.et.evaluator.api.TaskletMsgHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 27/11/2017.
 */
public final class DefaultTaskletMsgHandler implements TaskletMsgHandler {
  private static final Logger LOG = Logger.getLogger(DefaultTaskletMsgHandler.class.getName());

  @Inject
  private DefaultTaskletMsgHandler() {

  }

  @Override
  public void onNext(final byte[] bytes) {
    LOG.log(Level.FINE, "DefaultTaskletMsgHandler received a msg.");
  }
}
