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
package edu.snu.cay.services.em;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class TestUtils {

  /**
   * Should not be instantiated.
   */
  private TestUtils() {
  }

  /**
   * Use a thread pool to concurrently execute threads.
   * Note that this method does NOT wait for the termination of all threads before returning.
   */
  public static void runConcurrently(final Runnable[] threads) throws InterruptedException {
    final ExecutorService pool = Executors.newFixedThreadPool(threads.length);
    for (final Runnable thread : threads) {
      pool.submit(thread);
    }
    pool.shutdown();
  }
}
