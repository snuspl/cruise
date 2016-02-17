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
package edu.snu.cay.services.evalmanager.impl;

import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link HomogeneousEvalManager} requests for evaluators and handles REEF events correctly.
 */
public final class HomogeneousEvalManagerTest {
  private static final String EVAL_PREFIX = "eval-";
  private static final String CONTEXT_A_ID = "A";
  private static final String CONTEXT_B_ID = "B";
  private static final String CONTEXT_C_ID = "C";
  private static final String CONTEXT_D_ID = "D";
  private static final int THREAD_POOL_SIZE = 5;
  private static final int MAX_SLEEP_MILLIS = 1000;

  private EvaluatorManager evaluatorManager;
  private EventHandler<AllocatedEvaluator> driverAEHandler;
  private EventHandler<ActiveContext> driverACHandler;
  private EStage<Object> eventStage;

  private AtomicInteger evalCounter;
  private CountDownLatch validationCounter;

  @Before
  public void setUp() throws InjectionException {
    final EvaluatorRequestor mockedEvaluatorRequestor = mock(EvaluatorRequestor.class);

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) {
        final EvaluatorRequest request = (EvaluatorRequest) invocationOnMock.getArguments()[0];

        for (int i = 0; i < request.getNumber(); i++) {
          final String evalId = EVAL_PREFIX + evalCounter.getAndIncrement();
          eventStage.onNext(generateMockedEvaluator(evalId));
        }
        return null;
      }
    }).when(mockedEvaluatorRequestor).submit(any(EvaluatorRequest.class));

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(EvaluatorRequestor.class, mockedEvaluatorRequestor);
    evaluatorManager = injector.getInstance(HomogeneousEvalManager.class);

    driverAEHandler = new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        evaluatorManager.onEvent(allocatedEvaluator);
      }
    };
    driverACHandler = new EventHandler<ActiveContext>() {
      @Override
      public void onNext(final ActiveContext activeContext) {
        evaluatorManager.onEvent(activeContext);
      }
    };
    eventStage = new ThreadPoolStage<>(new EventHandler<Object>() {
      @Override
      public void onNext(final Object o) {
        try {
          // simulates messaging heartbeat to driver
          Thread.sleep(RandomUtils.nextInt(MAX_SLEEP_MILLIS));
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }

        if (o instanceof AllocatedEvaluator) {
          driverAEHandler.onNext((AllocatedEvaluator) o);
        } else {
          driverACHandler.onNext((ActiveContext) o);
        }
      }
    }, THREAD_POOL_SIZE);
    evalCounter = new AtomicInteger(0);
  }

  /**
   * Tests multiple event handling plans.
   * Requests for two groups of evaluators, submits different contexts on them,
   * and checks that context stack is built correctly.
   */
  @Test
  public void testEvalManager() {
    // Context Stack: A -> B -> C
    final String[] plan1 = new String[]{CONTEXT_A_ID, CONTEXT_B_ID, CONTEXT_C_ID};
    final int plan1Num = 3;
    final List<EventHandler<ActiveContext>> plan1ACHandlerList = new ArrayList<>();
    for (int i = 1; i < plan1.length; i++) {
      plan1ACHandlerList.add(new SubmitContextToACHandler(plan1[i]));
    }
    plan1ACHandlerList.add(new ContextStackValidationHandler(plan1[0] + plan1[1] + plan1[2]));

    // Context Stack: D -> A -> B -> C
    final String[] plan2 = new String[]{CONTEXT_D_ID, CONTEXT_A_ID, CONTEXT_B_ID, CONTEXT_C_ID};
    final int plan2Num = 8;
    final List<EventHandler<ActiveContext>> plan2ACHandlerList = new ArrayList<>();
    for (int i = 1; i < plan2.length; i++) {
      plan2ACHandlerList.add(new SubmitContextToACHandler(plan2[i]));
    }
    plan2ACHandlerList.add(new ContextStackValidationHandler(plan2[0] + plan2[1] + plan2[2] + plan2[3]));

    validationCounter = new CountDownLatch(plan1Num + plan2Num);
    evaluatorManager.allocateEvaluators(plan1Num, new SubmitContextToAEHandler(plan1[0]), plan1ACHandlerList);
    evaluatorManager.allocateEvaluators(plan2Num, new SubmitContextToAEHandler(plan2[0]), plan2ACHandlerList);

    try {
      validationCounter.await();
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate mocked {@link AllocatedEvaluator}, which provides {@code getId()} and {@code submitContext()} methods.
   * @param evalId evaluator identifier
   * @return mocked {@link AllocatedEvaluator}
   */
  private AllocatedEvaluator generateMockedEvaluator(final String evalId) {
    final AllocatedEvaluator mockedEvaluator = mock(AllocatedEvaluator.class);
    when(mockedEvaluator.getId()).thenReturn(evalId);
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws InjectionException {
        final Configuration conf = (Configuration) invocationOnMock.getArguments()[0];
        eventStage.onNext(generateMockedContext(conf, evalId));
        return null;
      }
    }).when(mockedEvaluator).submitContext(any(Configuration.class));
    return mockedEvaluator;
  }

  /**
   * Generate mocked {@link ActiveContext}, which provides {@code getId()} and {@code submitContext()} methods.
   * @param contextConf configuration for context, should include {@link ContextIdentifier}
   * @param evalId evaluator identifier
   * @return mocked {@link ActiveContext}
   * @throws InjectionException if {@code contextConf} does not include {@link ContextIdentifier}
   */
  private ActiveContext generateMockedContext(final Configuration contextConf,
                                              final String evalId) throws InjectionException {
    final ActiveContext mockedContext = mock(ActiveContext.class);
    final String contextId = Tang.Factory.getTang().newInjector(contextConf).getNamedInstance(ContextIdentifier.class);
    when(mockedContext.getId()).thenReturn(contextId);
    when(mockedContext.getEvaluatorId()).thenReturn(evalId);
    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) throws InjectionException {
        final Configuration conf = (Configuration) invocationOnMock.getArguments()[0];
        eventStage.onNext(generateMockedContext(conf, evalId));
        return null;
      }
    }).when(mockedContext).submitContext(any(Configuration.class));
    return mockedContext;
  }

  /**
   * {@link AllocatedEvaluator} handler which submits context with specified id in constructor.
   */
  final class SubmitContextToAEHandler implements EventHandler<AllocatedEvaluator> {
    private final String contextId;

    SubmitContextToAEHandler(final String contextId) {
      this.contextId = contextId;
    }

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final Configuration contextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, contextId)
          .build();
      allocatedEvaluator.submitContext(contextConf);
    }
  }

  /**
   * {@link ActiveContext} handler which submits context with following context identifier.
   * <i>top context id</i> + <i>specified id in constructor</i>.
   */
  final class SubmitContextToACHandler implements EventHandler<ActiveContext> {
    private final String contextId;

    SubmitContextToACHandler(final String contextId) {
      this.contextId = contextId;
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      final Configuration contextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, activeContext.getId() + contextId)
          .build();
      activeContext.submitContext(contextConf);
    }
  }

  /**
   * {@link ActiveContext} handler which checks whether context stack is built correctly.
   */
  final class ContextStackValidationHandler implements EventHandler<ActiveContext> {
    private final String expectedContextId;

    ContextStackValidationHandler(final String expectedContextId) {
      this.expectedContextId = expectedContextId;
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      assertEquals(expectedContextId, activeContext.getId());
      validationCounter.countDown();
    }
  }
}
