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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.dolphin.async.AsyncDolphinDriver;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.avro.ResultMsg;
import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.PlanExecutor;
import edu.snu.cay.services.em.plan.api.PlanResult;
import edu.snu.cay.services.em.plan.impl.PlanImpl;
import edu.snu.cay.services.em.plan.impl.PlanResultImpl;
import edu.snu.cay.services.em.plan.impl.TransferStepImpl;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.impl.Tuple2;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Unit tests for async dolphin plan executor.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AsyncDolphinDriver.class)
public final class AsyncDolphinPlanExecutorTest {

  private static PlanExecutor planExecutor;

  private static AsyncDolphinDriver driver;

  private final AtomicInteger idInt = new AtomicInteger();

  private static final String EVAL_PREFIX = "EVAL";


  @Before
  public void setUp() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(PlanExecutor.class, AsyncDolphinPlanExecutor.class)
            .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    driver = mock(AsyncDolphinDriver.class);
    when(driver.getEvalAllocHandlerForServer()).thenReturn(new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        allocatedEvaluator.submitContext(mock(Configuration.class));
      }
    });

    when(driver.getEvalAllocHandlerForWorker()).thenReturn(new EventHandler<AllocatedEvaluator>() {
      @Override
      public void onNext(final AllocatedEvaluator allocatedEvaluator) {
        allocatedEvaluator.submitContext(mock(Configuration.class));
      }
    });

    when(driver.getFirstContextActiveHandlerForServer(Mockito.anyBoolean()))
            .thenReturn(new EventHandler<ActiveContext>() {
              @Override
              public void onNext(final ActiveContext activeContext) {
                return;
              }
            });
    when(driver.getSecondContextActiveHandlerForServer())
            .thenReturn(new EventHandler<ActiveContext>() {
              @Override
              public void onNext(final ActiveContext activeContext) {
                return;
              }
            });
    when(driver.getFirstContextActiveHandlerForWorker(Mockito.anyBoolean()))
            .thenReturn(new EventHandler<ActiveContext>() {
              @Override
              public void onNext(final ActiveContext activeContext) {
                return;
              }
            });
    when(driver.getSecondContextActiveHandlerForWorker())
            .thenReturn(new EventHandler<ActiveContext>() {
              @Override
              public void onNext(final ActiveContext activeContext) {
                return;
              }
            });

    injector.bindVolatileInstance(AsyncDolphinDriver.class, driver);
    injector.bindVolatileParameter(ServerEM.class, new MockElasticMemory());
    injector.bindVolatileParameter(WorkerEM.class, new MockElasticMemory());

    try {
      planExecutor = injector.getInstance(PlanExecutor.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("InjectionException while injecting a plan executor: " + e);
    }
  }

  @Test
  public void simplePlanExecutionTest() {
    final Plan plan = PlanImpl.newBuilder()
            .addEvaluatorToDelete("WORKER", EVAL_PREFIX + 0)
            .addEvaluatorToAdd("WORKER", EVAL_PREFIX + 1)
            .addEvaluatorToAdd("SERVER", EVAL_PREFIX + 2)
            .addEvaluatorToDelete("SERVER", EVAL_PREFIX + 3)
            .addTransferStep("SERVER", new TransferStepImpl(EVAL_PREFIX + 4, EVAL_PREFIX + 2, new DataInfoImpl(1)))
            .build();

    final Future<PlanResult> result = planExecutor.execute(plan);
    final int planSize = plan.getPlanSize();

    try {
      final PlanResultImpl summary = (PlanResultImpl) result.get();
      assertEquals(planSize, summary.getNumExecutedOps());
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void complexDependencyPlanExecutionTest() {
    final Plan plan = PlanImpl.newBuilder()
            .addEvaluatorToDelete("SERVER", EVAL_PREFIX + 1)
            .addEvaluatorToDelete("SERVER", EVAL_PREFIX + 3)
            .addEvaluatorToDelete("WORKER", EVAL_PREFIX + 7)
            .addEvaluatorToAdd("SERVER", EVAL_PREFIX + 2)
            .addEvaluatorToAdd("WORKER", EVAL_PREFIX + 5)
            .addEvaluatorToAdd("WORKER", EVAL_PREFIX + 6)
            .addTransferStep("SERVER", new TransferStepImpl(EVAL_PREFIX + 4, EVAL_PREFIX + 2, new DataInfoImpl(1)))
            .addTransferStep("WORKER", new TransferStepImpl(EVAL_PREFIX + 8, EVAL_PREFIX + 9, new DataInfoImpl(1)))
            .build();

    final Future<PlanResult> result = planExecutor.execute(plan);
    final int planSize = plan.getPlanSize();

    try {
      final PlanResultImpl summary = (PlanResultImpl) result.get();
      assertEquals(planSize, summary.getNumExecutedOps());
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  private final class MockElasticMemory implements ElasticMemory {

    private final ConcurrentLinkedQueue<EventHandler<AvroElasticMemoryMessage>> deleteQueue;
    private final ConcurrentLinkedQueue<Tuple2<EventHandler<AllocatedEvaluator>,
            List<EventHandler<ActiveContext>>>> addQueue;
    private final ConcurrentLinkedQueue<EventHandler<AvroElasticMemoryMessage>> moveQueue;

    private MockElasticMemory() {
      deleteQueue = new ConcurrentLinkedQueue<>();
      addQueue = new ConcurrentLinkedQueue<>();
      moveQueue = new ConcurrentLinkedQueue<>();
      initializeDelete();
      initializeAdd();
      initializeMove();
    }

    private void initializeDelete() {
      final AvroElasticMemoryMessage msg = mock(AvroElasticMemoryMessage.class);
      final ResultMsg resultMsg = mock(ResultMsg.class);
      when(resultMsg.getResult()).thenReturn(Result.SUCCESS);
      when(msg.getResultMsg()).thenReturn(resultMsg);

      final Thread deletion = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            if (!deleteQueue.isEmpty()) {
              final EventHandler<AvroElasticMemoryMessage> deleteHandler = deleteQueue.poll();
              deleteHandler.onNext(msg);
            }
          }
        }
      });
      deletion.start();
    }

    private void initializeAdd() {

      final AllocatedEvaluator evaluator = mock(AllocatedEvaluator.class);

      when(evaluator.getId()).thenReturn("instance" + idInt.getAndIncrement());

      final ActiveContext context = mock(ActiveContext.class);
      final String evalId = evaluator.getId();
      when(context.getId()).thenReturn("" + idInt.getAndIncrement());
      when(context.getEvaluatorId()).thenReturn(evalId);
      doNothing().when(context).submitContextAndService(mock(Configuration.class), mock(Configuration.class));

      final Thread addition = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            if (!addQueue.isEmpty()) {
              final Tuple2 addHandlers = addQueue.poll();
              final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler
                      = (EventHandler<AllocatedEvaluator>) addHandlers.getT1();
              final List<EventHandler<ActiveContext>> ctxHandlerList
                      = (List<EventHandler<ActiveContext>>) addHandlers.getT2();

              final Answer todo = new Answer() {
                @Override
                public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                  for (final EventHandler<ActiveContext> handler : ctxHandlerList) {
                    handler.onNext(context);
                  }
                  return null;
                }
              };
              doAnswer(todo).when(evaluator).submitContext(Mockito.any(Configuration.class));

              evaluatorAllocatedHandler.onNext(evaluator);
            }
          }
        }
      });
      addition.start();
    }

    private void initializeMove() {
      final AvroElasticMemoryMessage msg = mock(AvroElasticMemoryMessage.class);
      final ResultMsg resultMsg = mock(ResultMsg.class);
      when(resultMsg.getResult()).thenReturn(Result.SUCCESS);
      when(msg.getResultMsg()).thenReturn(resultMsg);

      final Thread move = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            if (!moveQueue.isEmpty()) {
              final EventHandler<AvroElasticMemoryMessage> moveHandler = moveQueue.poll();
              moveHandler.onNext(msg);
            }
          }
        }
      });
      move.start();
    }

    @Override
    public void add(final int number, final int megaBytes, final int cores,
                    final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler,
                    final List<EventHandler <ActiveContext>> contextActiveHandlerList) {
      addQueue.add(new Tuple2<>(evaluatorAllocatedHandler, contextActiveHandlerList));
    }

    @Override
    public void delete(final String evalId, @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
      deleteQueue.add(callback);
    }

    @Override
    public void resize(final String evalId, final int megaBytes, final int cores) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void move(final int numBlocks, final String srcEvalId, final String destEvalId,
                     @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
      moveQueue.add(finishedCallback);
    }

    @Override
    public void checkpoint(final String evalId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerRoutingTableUpdateCallback(final String clientId,
                                                   final EventHandler<EMRoutingTableUpdate> updateCallback) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deregisterRoutingTableUpdateCallback(final String clientId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Set<Integer>> getStoreIdToBlockIds() {
      return null;
    }

  }

}
