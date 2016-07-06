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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * Unit tests for async dolphin plan executor.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(AsyncDolphinDriver.class)
public final class AsyncDolphinPlanExecutorTest {

  private static PlanExecutor planExecutor;

  private static final String EVAL_PREFIX = "EVAL";

  @Before
  public void setUp() throws InjectionException {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(PlanExecutor.class, AsyncDolphinPlanExecutor.class)
            .build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    final AsyncDolphinDriver driver = mock(AsyncDolphinDriver.class);
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
      final PlanResult summary = result.get(10, TimeUnit.SECONDS);
      assertEquals(planSize, summary.getNumExecutedOps());
    } catch (final TimeoutException e) {
      fail("Failed to execute the plan. Timeout occurred.");
      e.printStackTrace();
    } catch (final InterruptedException | ExecutionException e) {
      fail("Failed to execute the plan. Exception occurred.");
      e.printStackTrace();
    }
  }

  @Test
  public void complexDependencyPlanExecutionTest() {
    // The plan below is composed of operations in the order of dependency
    final Plan plan = PlanImpl.newBuilder()
            .addEvaluatorToDelete("SERVER", EVAL_PREFIX + 1)
            .addEvaluatorToDelete("SERVER", EVAL_PREFIX + 2)
            .addEvaluatorToDelete("WORKER", EVAL_PREFIX + 3)
            .addTransferStep("WORKER", new TransferStepImpl(EVAL_PREFIX + 4, EVAL_PREFIX + 5, new DataInfoImpl(1)))
            .addEvaluatorToAdd("SERVER", EVAL_PREFIX + 6)
            .addEvaluatorToAdd("WORKER", EVAL_PREFIX + 7)
            .addEvaluatorToAdd("WORKER", EVAL_PREFIX + 8)
            .addTransferStep("SERVER", new TransferStepImpl(EVAL_PREFIX + 9, EVAL_PREFIX + 6, new DataInfoImpl(1)))
            .build();

    final Future<PlanResult> result = planExecutor.execute(plan);
    final int planSize = plan.getPlanSize();

    try {
      final PlanResult summary = result.get(10, TimeUnit.SECONDS);
      assertEquals(planSize, summary.getNumExecutedOps());
    } catch (final TimeoutException e) {
      fail("Failed to execute the plan. Timeout occurred.");
      e.printStackTrace();
    } catch (final InterruptedException | ExecutionException e) {
      fail("Failed to execute the plan. Exception occurred.");
      e.printStackTrace();
    }
  }

  private final class MockElasticMemory implements ElasticMemory {

    private final ConcurrentLinkedQueue<EventHandler<AvroElasticMemoryMessage>> deleteHandlerQueue;
    private final ConcurrentLinkedQueue<Tuple2<EventHandler<AllocatedEvaluator>,
            List<EventHandler<ActiveContext>>>> addHandlerQueue;
    private final ConcurrentLinkedQueue<EventHandler<AvroElasticMemoryMessage>> moveHandlerQueue;

    private final AtomicInteger idInt = new AtomicInteger();

    private MockElasticMemory() {
      deleteHandlerQueue = new ConcurrentLinkedQueue<>();
      addHandlerQueue = new ConcurrentLinkedQueue<>();
      moveHandlerQueue = new ConcurrentLinkedQueue<>();
      initDeleteThread();
      initAddThread();
      initMoveThread();
    }

    private void initDeleteThread() {
      final AvroElasticMemoryMessage msg = mock(AvroElasticMemoryMessage.class);
      final ResultMsg resultMsg = mock(ResultMsg.class);
      when(resultMsg.getResult()).thenReturn(Result.SUCCESS);
      when(msg.getResultMsg()).thenReturn(resultMsg);

      final Thread deletion = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            final EventHandler<AvroElasticMemoryMessage> deleteHandler = deleteHandlerQueue.poll();
            if (deleteHandler != null) {
              deleteHandler.onNext(msg);
            }
          }
        }
      });
      deletion.start();
    }

    private void initAddThread() {

      final AllocatedEvaluator evaluator = mock(AllocatedEvaluator.class);

      when(evaluator.getId()).thenReturn("instance" + idInt.getAndIncrement());

      final ActiveContext context = mock(ActiveContext.class);
      final String evalId = evaluator.getId();
      when(context.getId()).thenReturn(Integer.toString(idInt.getAndIncrement()));
      when(context.getEvaluatorId()).thenReturn(evalId);
      doNothing().when(context).submitContextAndService(mock(Configuration.class), mock(Configuration.class));

      final Thread addition = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            final Tuple2 addHandlers = addHandlerQueue.poll();
            if (addHandlers != null) {
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

    private void initMoveThread() {
      final AvroElasticMemoryMessage msg = mock(AvroElasticMemoryMessage.class);
      final ResultMsg resultMsg = mock(ResultMsg.class);
      when(resultMsg.getResult()).thenReturn(Result.SUCCESS);
      when(msg.getResultMsg()).thenReturn(resultMsg);

      final Thread move = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            final EventHandler<AvroElasticMemoryMessage> moveHandler = moveHandlerQueue.poll();
            if (moveHandler != null) {
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
      addHandlerQueue.add(new Tuple2<>(evaluatorAllocatedHandler, contextActiveHandlerList));
    }

    @Override
    public void delete(final String evalId, @Nullable final EventHandler<AvroElasticMemoryMessage> callback) {
      deleteHandlerQueue.add(callback);
    }

    @Override
    public void resize(final String evalId, final int megaBytes, final int cores) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void move(final int numBlocks, final String srcEvalId, final String destEvalId,
                     @Nullable final EventHandler<AvroElasticMemoryMessage> finishedCallback) {
      moveHandlerQueue.add(finishedCallback);
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
