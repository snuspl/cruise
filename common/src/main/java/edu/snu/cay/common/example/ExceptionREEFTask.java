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
package edu.snu.cay.common.example;

import edu.snu.cay.utils.CatchableExecutors;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A task code for Exception REEF app.
 */
public final class ExceptionREEFTask implements Task {


  private static final Logger LOG = Logger.getLogger(ExceptionREEFTask.class.getName());

  @Inject
  private ExceptionREEFTask() {

  }

  /**
   * Checks that the exception thrown from the user-thread propagates itself
   * to the main REEF-thread and eventually the REEF job fails.
   */
  @Override
  public byte[] call(final byte[] bytes) throws Exception {

    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      for (int i = 1; i <= 100; i++) {
        LOG.log(Level.INFO, "Test print : [{0} / 200]", i);
      }

      // it occurs divide-by-zero exception
      final int errorNumber = 1 / 0;

      for (int i = 101; i <= 200; i++) {
        LOG.log(Level.INFO, "Test print : [{0} / 200]", i);
      }
    });

    Thread.sleep(1000);
    return null;
  }
}
