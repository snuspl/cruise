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
package edu.snu.cay.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public final class ThreadUtils {

  /**
   * Should not be instantiated.
   */
  private ThreadUtils() {
  }

  /**
   * Use a thread pool to concurrently execute threads.
   * Note that this method does NOT wait for the termination of all threads before returning.
   * @return a list of futures
   */
  public static List<Future> runConcurrently(final Runnable[] threads) throws InterruptedException {

    final List<Future> futures = new ArrayList<>(threads.length);

    final ExecutorService pool = CatchableExecutors.newFixedThreadPool(threads.length);
    for (final Runnable thread : threads) {
      final Future future = pool.submit(thread);
      futures.add(future);
    }
    pool.shutdown();
    return futures;
  }

  /**
   * Use a thread pool to concurrently execute threads.
   * It returns futures of submitted threads.
   * Note that this method does NOT wait for the termination of all threads before returning.
   * @return a list of futures
   */
  public static <T> List<Future<T>> runConcurrentlyWithResult(final List<Callable<T>> threads) {

    final List<Future<T>> futures = new ArrayList<>(threads.size());

    final ExecutorService pool = CatchableExecutors.newFixedThreadPool(threads.size());
    for (final Callable<T> thread : threads) {
      final Future<T> future = pool.submit(thread);
      futures.add(future);
    }
    pool.shutdown();
    return futures;
  }

  /**
   * Retrieves the actual values from a list of futures.
   * @param futures list of futures returned from the concurrent execution
   * @param <T> type of the actual result
   * @return a list of actual values of results.
   */
  public static <T> List<T> retrieveResults(final List<Future<T>> futures) {
    return futures.stream().map((Future<T> x) -> {
      try {
        return x.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }
}
