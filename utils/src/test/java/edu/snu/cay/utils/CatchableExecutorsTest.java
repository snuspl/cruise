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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Test for the {@link CatchableExecutors} class.
 */
public class CatchableExecutorsTest {

  private final List<Throwable> errorList = new ArrayList<>();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    errorList.clear();
    Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> errorList.add(throwable));
  }

  @Test
  public void newSingleThreadExecutorTest() throws InterruptedException {
    thrown.expect(RuntimeException.class);
    CatchableExecutors.newSingleThreadExecutor().submit(() -> {
      final int errorNumber = 1 / 0;
    });

    Thread.sleep(500);

    errorList.forEach(throwable -> {
      throw new RuntimeException(throwable);
    });
  }

  @Test
  public void newFixedThreadPoolTest() throws InterruptedException {
    thrown.expect(RuntimeException.class);
    final ExecutorService threadPool = CatchableExecutors.newFixedThreadPool(4);
    for (int index = 0; index < 3; index++) {
      threadPool.submit(() -> {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
    }

    threadPool.submit(() -> {
      final int errorNumber = 1 / 0;
    });

    Thread.sleep(500);

    errorList.forEach(throwable -> {
      throw new RuntimeException(throwable);
    });
  }
}
