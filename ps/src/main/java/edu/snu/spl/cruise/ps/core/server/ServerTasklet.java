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
package edu.snu.spl.cruise.ps.core.server;

import edu.snu.spl.cruise.services.et.evaluator.api.Tasklet;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Server-side tasklet implementation that does nothing.
 */
public final class ServerTasklet implements Tasklet {
  private static final Logger LOG = Logger.getLogger(ServerTasklet.class.getName());
  public static final String TASKLET_ID = "ServerTasklet";

  /**
   * A latch that will be released upon {@link #close()}.
   * Then {@link #call(byte[])} will complete and the task will finish.
   */
  private final CountDownLatch closeLatch = new CountDownLatch(1);

  @Inject
  private ServerTasklet() {
  }

  @Override
  public void run() throws Exception {
    closeLatch.await();
  }

  /**
   * Called when the Task is requested to close.
   * The {@link #closeLatch} is released, so the task terminates execution.
   */
  @Override
  public void close() {
    LOG.log(Level.INFO, "Requested to close!");
    closeLatch.countDown();
  }
}
