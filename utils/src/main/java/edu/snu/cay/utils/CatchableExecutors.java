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


import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class a extension version of {@link ExecutorService}.
 * It can spawn user threads which futures are managed by {@code #threadFutureMap}
 */
public final class CatchableExecutors {

  private static final Logger LOG = Logger.getLogger(CatchableExecutors.class.getName());

  private static CatchableExecutors instance;
  private final AtomicInteger defaultThreadIdentifier = new AtomicInteger(0);
  private final Map<String, Future> threadFutureMap;
  private Thread mainThread;

  private CatchableExecutors(final Thread mainThread) {
    this.threadFutureMap = new ConcurrentHashMap<>();
    this.mainThread = mainThread;

    final Thread poolThread = new Thread(() -> {
      while (true) {
        if (threadFutureMap.size() > 0) {
          threadFutureMap.forEach((threadId, future) -> {
            if (future.isDone()) {
              try {
                future.get();
              } catch (InterruptedException | ExecutionException e) {
                LOG.log(Level.INFO, "Exception caught, thread id : {0}, exception : {1}",
                    new Object[]{threadId, e});
                //this.mainThread.interrupt();
                throw new RuntimeException(e);
              }
            }
          });
        }
      }
    });

    poolThread.start();
  }

  public static CatchableExecutors getInstance() {
    if (instance == null) {
      instance = new CatchableExecutors(Thread.currentThread());
    }
    return instance;
  }

  public CatchableExecutors setMainThread(final Thread nextThread) {
    this.mainThread = nextThread;
    return instance;
  }

  public void submitBySingle(final String threadId, final Runnable runnable) {
    final Future result = Executors.newSingleThreadExecutor().submit(runnable);
    threadFutureMap.put(threadId, result);
  }

  public void submitBySingle(final Runnable runnable) {
    final String threadId = String.valueOf(defaultThreadIdentifier.getAndIncrement());
    submitBySingle(threadId, runnable);
  }

}
