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

import com.google.common.collect.Lists;
import edu.snu.cay.services.evalmanager.api.EvaluatorManager;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.ThreadPoolStage;
import org.apache.reef.wake.remote.impl.Tuple2;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests that {@link HeterogeneousEvalManager} requests for evaluators and handles REEF events correctly.
 */
public final class HeterogeneousEvalManagerTest {
  private static final String EVAL_PREFIX = "eval-";
  private static final String CONTEXT_A_ID = "A";
  private static final String CONTEXT_B_ID = "B";
  private static final String CONTEXT_C_ID = "C";
  private static final String CONTEXT_D_ID = "D";
  private static final int THREAD_POOL_SIZE = 5;
  private static final int MAX_SLEEP_MILLIS = 20;
  private static final int TEST_TIMEOUT_MILLIS = 60000;
  private static final int EVAL_MEM_SIZE = 128;
  private static final int EVAL_NUM_CORES = 1;

  private EvaluatorManager evaluatorManager;
  private final EStage<Object> eventStage = new ThreadPoolStage<>(new EventHandler<Object>() {
    @Override
    public void onNext(final Object o) {
      try {
        // simulates messaging heartbeat to driver, to shuffle executing order randomly
        Thread.sleep(RandomUtils.nextInt(MAX_SLEEP_MILLIS));
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }

      if (o instanceof AllocatedEvaluator) {
        evaluatorManager.onEvaluatorAllocated((AllocatedEvaluator) o);
      } else {
        evaluatorManager.onContextActive((ActiveContext) o);
      }
    }
  }, THREAD_POOL_SIZE);

  private AtomicInteger evalCounter;
  private CountDownLatch finishedCounter;
  private Map<String, List<String>> evalIdToActualContextIdStack;
  private Map<String, List<String>> evalIdToExpectedContextIdStack;

  @Before
  public void setUp() throws InjectionException {
    evaluatorManager = new HeterogeneousEvalManager(generateMockedEvaluatorRequestor());
    evalCounter = new AtomicInteger(0);
    evalIdToActualContextIdStack = new ConcurrentHashMap<>();
    evalIdToExpectedContextIdStack = new ConcurrentHashMap<>();
  }

  /**
   * Tests single plan with a single context submit.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanSingleContext() {
    final List<String> plan = Lists.newArrayList(CONTEXT_A_ID);
    final int evalNum = 3;
    final Tuple2<EventHandler<AllocatedEvaluator>, List<EventHandler<ActiveContext>>> handlers
        = getHandlersFromPlan(plan);
    finishedCounter = new CountDownLatch(evalNum);

    evaluatorManager.allocateEvaluators(evalNum, EVAL_MEM_SIZE, EVAL_NUM_CORES, handlers.getT1(), handlers.getT2());

    try {
      finishedCounter.await(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (final String evalId : evalIdToActualContextIdStack.keySet()) {
      final List<String> expectedContextIdStack = evalIdToExpectedContextIdStack.get(evalId);
      final List<String> actualContextIdStack = evalIdToActualContextIdStack.get(evalId);
      assertEquals(expectedContextIdStack, actualContextIdStack);
    }
  }

  /**
   * Tests single plan with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanMultipleContext() {
    final List<String> plan = Lists.newArrayList(CONTEXT_A_ID, CONTEXT_B_ID, CONTEXT_C_ID, CONTEXT_D_ID);
    final int evalNum = 3;
    final Tuple2<EventHandler<AllocatedEvaluator>, List<EventHandler<ActiveContext>>> handlers
        = getHandlersFromPlan(plan);
    finishedCounter = new CountDownLatch(evalNum);

    evaluatorManager.allocateEvaluators(evalNum, EVAL_MEM_SIZE, EVAL_NUM_CORES, handlers.getT1(), handlers.getT2());

    try {
      finishedCounter.await(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (final String evalId : evalIdToActualContextIdStack.keySet()) {
      final List<String> expectedContextIdStack = evalIdToExpectedContextIdStack.get(evalId);
      final List<String> actualContextIdStack = evalIdToActualContextIdStack.get(evalId);
      assertEquals(expectedContextIdStack, actualContextIdStack);
    }
  }

  /**
   * Tests multiple plans with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testMultiplePlanMultipleContext() {
    // Context Stack: A -> B -> C
    final List<String> plan1 = Lists.newArrayList(CONTEXT_A_ID, CONTEXT_B_ID, CONTEXT_C_ID);
    final int evalNumForPlan1 = 500;
    final Tuple2<EventHandler<AllocatedEvaluator>, List<EventHandler<ActiveContext>>> handlersForPlan1
        = getHandlersFromPlan(plan1);

    // Context Stack: D -> A -> B -> C
    final List<String> plan2 = Lists.newArrayList(CONTEXT_D_ID, CONTEXT_A_ID, CONTEXT_B_ID, CONTEXT_C_ID);
    final int evalNumForPlan2 = 500;
    final Tuple2<EventHandler<AllocatedEvaluator>, List<EventHandler<ActiveContext>>> handlersForPlan2
        = getHandlersFromPlan(plan2);

    // Context Stack: B -> D -> A -> A
    final List<String> plan3 = Lists.newArrayList(CONTEXT_B_ID, CONTEXT_D_ID, CONTEXT_A_ID, CONTEXT_A_ID);
    final int evalNumForPlan3 = 500;
    final Tuple2<EventHandler<AllocatedEvaluator>, List<EventHandler<ActiveContext>>> handlersForPlan3
        = getHandlersFromPlan(plan3);

    finishedCounter = new CountDownLatch(evalNumForPlan1 + evalNumForPlan2 + evalNumForPlan3);

    evaluatorManager.allocateEvaluators(evalNumForPlan1, EVAL_MEM_SIZE, EVAL_NUM_CORES,
        handlersForPlan1.getT1(), handlersForPlan1.getT2());
    evaluatorManager.allocateEvaluators(evalNumForPlan2, EVAL_MEM_SIZE, EVAL_NUM_CORES,
        handlersForPlan2.getT1(), handlersForPlan2.getT2());
    evaluatorManager.allocateEvaluators(evalNumForPlan3, EVAL_MEM_SIZE, EVAL_NUM_CORES,
        handlersForPlan3.getT1(), handlersForPlan3.getT2());

    try {
      finishedCounter.await(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
    for (final String evalId : evalIdToActualContextIdStack.keySet()) {
      final List<String> expectedContextIdStack = evalIdToExpectedContextIdStack.get(evalId);
      final List<String> actualContextIdStack = evalIdToActualContextIdStack.get(evalId);
      assertEquals(expectedContextIdStack, actualContextIdStack);
    }
  }

  /**
   * Tests multiple requests for heterogeneous evaluators.
   * Checks that allocated evaluators's resource type is as requested.
   */
  @Test
  public void testHeteroEvalRequest() {
    final int evalNumForReq1 = 500;
    final int evalNumForReq2 = 500;
    final int evalNumForReq3 = 500;

    final int numCoresForReq1 = 1;
    final int numCoresForReq2 = 1;
    final int numCoresForReq3 = 1;
    final int memSizeForReq1 = 100;
    final int memSizeForReq2 = 200;
    final int memSizeForReq3 = 300;

    finishedCounter = new CountDownLatch(evalNumForReq1 + evalNumForReq2 + evalNumForReq3);

    evaluatorManager.allocateEvaluators(evalNumForReq1, memSizeForReq1, numCoresForReq1,
        new EvalTypeChecker(numCoresForReq1, memSizeForReq1, finishedCounter), Collections.emptyList());
    evaluatorManager.allocateEvaluators(evalNumForReq2, memSizeForReq2, numCoresForReq2,
        new EvalTypeChecker(numCoresForReq2, memSizeForReq2, finishedCounter), Collections.emptyList());
    evaluatorManager.allocateEvaluators(evalNumForReq3, memSizeForReq3, numCoresForReq3,
        new EvalTypeChecker(numCoresForReq3, memSizeForReq3, finishedCounter), Collections.emptyList());

    try {
      finishedCounter.await(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate mocked {@link EvaluatorRequestor}, which provides {@code submit()} method.
   * @return mocked {@link EvaluatorRequestor}
   */
  private EvaluatorRequestor generateMockedEvaluatorRequestor() {
    final EvaluatorRequestor mockedEvaluatorRequestor = mock(EvaluatorRequestor.class);

    doAnswer(new Answer() {
      @Override
      public Object answer(final InvocationOnMock invocationOnMock) {
        final EvaluatorRequest request = (EvaluatorRequest) invocationOnMock.getArguments()[0];
        requestMockedEvaluators(request);
        return null;
      }
    }).when(mockedEvaluatorRequestor).submit(any(EvaluatorRequest.class));

    return mockedEvaluatorRequestor;
  }

  /**
   * Request for mocked evaluators, and hand them over to {@code eventStage}.
   */
  private void requestMockedEvaluators(final EvaluatorRequest evaluatorRequest) {
    for (int i = 0; i < evaluatorRequest.getNumber(); i++) {
      eventStage.onNext(generateMockedEvaluator(evaluatorRequest.getNumberOfCores(), evaluatorRequest.getMegaBytes()));
    }
  }

  /**
   * Generate mocked {@link AllocatedEvaluator}, which provides {@code getId()} and {@code submitContext()} methods.
   * @param numCores the number of cores
   * @param memSizeInMB the memory size in MegaBytes
   * @return mocked {@link AllocatedEvaluator}
   */
  private AllocatedEvaluator generateMockedEvaluator(final int numCores, final int memSizeInMB) {
    final String evalId = EVAL_PREFIX + evalCounter.getAndIncrement();
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

    final EvaluatorDescriptor mockedEvalDescriptor = mock(EvaluatorDescriptor.class);
    when(mockedEvalDescriptor.getNumberOfCores()).thenReturn(numCores);
    when(mockedEvalDescriptor.getMemory()).thenReturn(memSizeInMB);
    when(mockedEvaluator.getEvaluatorDescriptor()).thenReturn(mockedEvalDescriptor);

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
   * Generate event handlers for given context stack plan.
   * @param plan list of context identifier
   * @return event handlers
   */
  private Tuple2<EventHandler<AllocatedEvaluator>,
      List<EventHandler<ActiveContext>>> getHandlersFromPlan(final List<String> plan) {
    final EventHandler<AllocatedEvaluator> allocatedEvaluatorHandler = new SubmitContextToAE(plan.get(0), plan);
    final List<EventHandler<ActiveContext>> activeContextHandlerList = new ArrayList<>();
    for (int i = 1; i < plan.size(); i++) {
      activeContextHandlerList.add(new SubmitContextToAC(plan.get(i)));
    }
    activeContextHandlerList.add(new LastACHandler());
    return new Tuple2<>(allocatedEvaluatorHandler, activeContextHandlerList);
  }

  /**
   * {@link AllocatedEvaluator} handler which checks the resource type is as requested.
   */
  private final class EvalTypeChecker implements EventHandler<AllocatedEvaluator> {
    private final int numCores;
    private final int memSizeInMB;
    private final CountDownLatch latch;

    EvalTypeChecker(final int numCores, final int memSizeInMB, final CountDownLatch latch) {
      this.numCores = numCores;
      this.memSizeInMB = memSizeInMB;
      this.latch = latch;
    }

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final EvaluatorDescriptor evalDesc = allocatedEvaluator.getEvaluatorDescriptor();
      assertEquals(evalDesc.getNumberOfCores(), numCores);
      assertEquals(evalDesc.getMemory(), memSizeInMB);
      latch.countDown();
    }
  }

  /**
   * {@link AllocatedEvaluator} handler which submits context with specified id in constructor.
   * Add a stack of context id for this evaluator to evalIdToExpectedContextIdStack and evalIdToActualContextIdStack.
   */
  private final class SubmitContextToAE implements EventHandler<AllocatedEvaluator> {
    private final String contextId;
    private final List<String> expectedContextIdStack;

    SubmitContextToAE(final String contextId, final List<String> expectedContextIdStack) {
      this.contextId = contextId;
      this.expectedContextIdStack = expectedContextIdStack;
    }

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      evalIdToExpectedContextIdStack.put(allocatedEvaluator.getId(), expectedContextIdStack);
      evalIdToActualContextIdStack.put(allocatedEvaluator.getId(), new ArrayList<String>());
      final Configuration contextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, contextId)
          .build();
      allocatedEvaluator.submitContext(contextConf);
    }
  }

  /**
   * {@link ActiveContext} handler which submits context with specified id in constructor.
   * Add identifier of given {@link ActiveContext} to evalIdToActualContextIdStack for validation.
   */
  private final class SubmitContextToAC implements EventHandler<ActiveContext> {
    private final String contextId;

    SubmitContextToAC(final String contextId) {
      this.contextId = contextId;
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      evalIdToActualContextIdStack.get(activeContext.getEvaluatorId()).add(activeContext.getId());
      final Configuration contextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, contextId)
          .build();
      activeContext.submitContext(contextConf);
    }
  }

  /**
   * Last {@link ActiveContext} handler which does not submit context.
   * Add identifier of given {@link ActiveContext} to evalIdToActualContextIdStack for validation.
   */
  private final class LastACHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      evalIdToActualContextIdStack.get(activeContext.getEvaluatorId()).add(activeContext.getId());
      finishedCounter.countDown();
    }
  }
}
