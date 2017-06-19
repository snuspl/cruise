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

import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Factory methods for {@link CatchableExecutor} as similar with {@link Executors}.
 */
public final class CatchableExecutors {

  /**
   * Creates an Executor that uses a single worker thread operating
   * off an unbounded queue. Unlike {@code Executors.newSingleThreadExecutor()}, if this
   * single thread terminates due to a failure during execution,
   * it just throws a {@link RuntimeException} rather than takes a new one.
   * @return the newly created single-threaded Executor
   */
  public static CatchableExecutor newSingleThreadExecutor() {
    return new CatchableExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  /**
   * Creates a thread pool that reuses a fixed number of threads operating
   * off a shared unbounded queue. At any point, at most {@code numThreads}
   * threads will be active processing tasks. Unlike {@code Executors.newFixedThreadPool()}, if any
   * thread terminates due to a failure during execution, it just throws a {@link RuntimeException}.
   * @param numThreads the number of threads in the pool
   * @return the newly created thread pool
   */
  public static CatchableExecutor newFixedThreadPool(final int numThreads) {
    return new CatchableExecutor(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  /**
   * This class is an extended version of {@link ThreadPoolExecutor}.
   * With this executorIt can spawn user threads that it throws {@link RuntimeException} when detecting any exception.
   * See {@link #afterExecute(Runnable, Throwable)}.
   */
  public static class CatchableExecutor extends ThreadPoolExecutor {
    private CatchableExecutor(final int corePoolSize, final int maximumPoolSize, final long keepAliveTime,
                               final TimeUnit unit, final BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    protected void afterExecute(final Runnable runnable, final Throwable throwable) {
      super.afterExecute(runnable, throwable);
      Throwable resultThrowable = throwable;
      if (throwable == null && runnable instanceof Future<?>) {
        try {
          ((Future) runnable).get();
        } catch (InterruptedException | ExecutionException e) {
          resultThrowable = e;
        }
      }

      if (resultThrowable != null) {
        throw new RuntimeException(resultThrowable);
      }
    }
  }
}
