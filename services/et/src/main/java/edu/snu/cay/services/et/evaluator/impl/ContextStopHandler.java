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

import org.apache.reef.evaluator.context.events.ContextStop;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class for handling event of closing an executor.
 */
public final class ContextStopHandler implements EventHandler<ContextStop> {
  private static final Logger LOG = Logger.getLogger(ContextStopHandler.class.getName());
  private final TaskletRuntime taskletRuntime;
  private final ChkpManagerSlave chkpManagerSlave;

  @Inject
  private ContextStopHandler(final TaskletRuntime taskletRuntime,
                             final ChkpManagerSlave chkpManagerSlave) {
    this.taskletRuntime = taskletRuntime;
    this.chkpManagerSlave = chkpManagerSlave;
  }

  @Override
  public void onNext(final ContextStop ctxStop) {
    LOG.log(Level.INFO, "Close context: {0}", ctxStop.getId());
    try {
      taskletRuntime.close();
      chkpManagerSlave.commitAllLocalChkps();
    } catch (IOException e) {
      throw new RuntimeException("Fail to commit blocks before closing", e);
    }
  }
}
