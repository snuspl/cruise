/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async;

import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import edu.snu.cay.services.ps.worker.api.ParameterWorker;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link MiniBatchParameterWorker} stores local updates, and communicates with {@link ParameterWorker}.
 */
public class MiniBatchParameterWorkerTest {
  private ParameterWorker<Integer, Integer, Integer> mockParameterWorker;
  private ParameterUpdater<Integer, Integer, Integer> mockParameterUpdater;
  private MiniBatchParameterWorker<Integer, Integer, Integer> miniBatchParameterWorker;
  private final Map<Integer, Integer> parameterMap = new HashMap<>();
  private final AtomicInteger dataFromParameterWorkerCount = new AtomicInteger();
  private final AtomicInteger dataToParameterWorkerCount = new AtomicInteger();

  @Before
  public void setUp() throws InjectionException {
    final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
        .build();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    mockParameterWorker = mock(ParameterWorker.class);
    injector.bindVolatileInstance(ParameterWorker.class, mockParameterWorker);
    mockParameterUpdater = mock(ParameterUpdater.class);
    injector.bindVolatileInstance(ParameterUpdater.class, mockParameterUpdater);
    miniBatchParameterWorker = injector.getInstance(MiniBatchParameterWorker.class);

    final Random generator = new Random();
    final int parametersCount = 10;
    for (int i = 0; i < parametersCount; i++) {
      final int value  = generator.nextInt();
      parameterMap.put(i, value);
    }

    doAnswer(invocation -> {
        dataFromParameterWorkerCount.incrementAndGet();
        final Integer key  = invocation.getArgumentAt(0, Integer.class);
        return parameterMap.get(key);
      }).when(mockParameterWorker).pull((Integer) anyObject());

    doAnswer(invocation -> {
        final List<Integer> keys  = invocation.getArgumentAt(0, List.class);
        final List<Integer> values = new ArrayList<>();
        for (final Integer key : keys) {
          dataFromParameterWorkerCount.incrementAndGet();
          values.add(parameterMap.get(key));
        }
        return values;
      }).when(mockParameterWorker).pull(anyList());

    doAnswer(invocation -> {
        dataToParameterWorkerCount.incrementAndGet();
        final Integer key = invocation.getArgumentAt(0, Integer.class);
        final Integer value = invocation.getArgumentAt(1, Integer.class);
        parameterMap.put(key, value);
        return null;
      }).when(mockParameterWorker).push(anyObject(), anyObject());

    doAnswer(invocation -> {
        return invocation.getArgumentAt(1, Integer.class);
      }).when(mockParameterUpdater).process(anyObject(), anyObject());
    doAnswer(invocation -> {
        return invocation.getArgumentAt(1, Integer.class);
      }).when(mockParameterUpdater).update(anyObject(), anyObject());
    doAnswer(invocation -> {
        final Integer preValue1 = invocation.getArgumentAt(0, Integer.class);
        final Integer preValue2 = invocation.getArgumentAt(1, Integer.class);
        return preValue1 + preValue2;
      }).when(mockParameterUpdater).aggregate(anyObject(), anyObject());
  }

  @Test
  public void testPull() {
    // Local cache doesn't contain any parameters at first.
    for (int i = 0; i < parameterMap.size() / 2; i++) {
      final Integer value = miniBatchParameterWorker.pull(i);
      assertEquals(parameterMap.get(i), value);
    }
    assertEquals(parameterMap.size() / 2, dataFromParameterWorkerCount.get());

    // Read parameter from local cache if there it is
    dataFromParameterWorkerCount.set(0);
    for (int i = 0; i < parameterMap.size() / 2; i++) {
      final Integer value = miniBatchParameterWorker.pull(i);
      assertEquals(parameterMap.get(i), value);
    }
    assertEquals(0, dataFromParameterWorkerCount.get());

    // Read subset of keys which are not in the local cache from ParameterWorker, otherwise from local cache
    dataFromParameterWorkerCount.set(0);
    final List<Integer> keys = new ArrayList<>(parameterMap.keySet());
    final List<Integer> values = miniBatchParameterWorker.pull(keys);
    int i = 0;
    for (final Integer key : keys) {
      assertEquals(parameterMap.get(key), values.get(i++));
    }
    assertEquals(parameterMap.size() / 2, dataFromParameterWorkerCount.get());
  }

  @Test
  public void testPush() {
    int parameterValue = 1;
    int aggregatedValue = parameterValue;
    for (int i = 0; i < parameterMap.size(); i++) {
      miniBatchParameterWorker.push(i, parameterValue);
    }
    assertEquals(0, dataToParameterWorkerCount.get());

    // local value is changed
    for (int i = 0; i < parameterMap.size(); i++) {
      assertEquals(parameterValue, miniBatchParameterWorker.pull(i).intValue());
    }

    parameterValue += 1;
    aggregatedValue += parameterValue;
    for (int i = 0; i < parameterMap.size(); i++) {
      miniBatchParameterWorker.push(i, parameterValue);
    }
    assertEquals(0, dataToParameterWorkerCount.get());

    miniBatchParameterWorker.flushLocalUpdates();
    assertEquals(parameterMap.size(), dataToParameterWorkerCount.get());
    // aggregated values are sent to the ParameterWorker
    for (int i = 0; i < parameterMap.size(); i++) {
      assertEquals(aggregatedValue, parameterMap.get(i).intValue());
    }
  }

}
