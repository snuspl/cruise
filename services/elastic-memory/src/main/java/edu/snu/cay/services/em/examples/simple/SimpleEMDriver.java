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
package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.common.centcomm.driver.AggregationManager;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.EMConfProvider;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.examples.simple.parameters.NumMoves;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for EMExample.
 * Driver runs a move between two randomly chosen evaluators.
 * The number of moves is configurable.
 */
@Unit
final class SimpleEMDriver {
  private static final Logger LOG = Logger.getLogger(SimpleEMDriver.class.getName());
  static final int NUM_EVAL = 3; // 3 is enough to cover all test cases (sender, receiver, other)
  private static final String CONTEXT_ID_PREFIX = "Context-";
  private static final String TASK_ID_PREFIX = "Task-";
  static final String AGGREGATION_CLIENT_ID = "AGGREGATION_CLIENT_ID";

  private final EvaluatorRequestor requestor;
  private final AggregationManager aggregationManager;
  private final EMConfProvider emConfProvider;
  private final HTraceParameters traceParameters;

  private final boolean rangeSupport;
  private final int numMoves;

  @Inject
  private SimpleEMDriver(final EvaluatorRequestor requestor,
                         final AggregationManager aggregationManager,
                         final EMConfProvider emConfProvider,
                         final HTraceParameters traceParameters,
                         @Parameter(RangeSupport.class) final boolean rangeSupport,
                         @Parameter(NumMoves.class) final int numMoves) throws InjectionException {
    this.requestor = requestor;
    this.aggregationManager = aggregationManager;
    this.emConfProvider = emConfProvider;
    this.traceParameters = traceParameters;
    this.rangeSupport = rangeSupport;
    this.numMoves = numMoves;
  }

  /**
   * Spawn small containers.
   */
  final class DriverStartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      SimpleEMDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(NUM_EVAL)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Configure allocated evaluators with EM configuration and give them id numbers.
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    private final AtomicInteger activeEvaluatorCount = new AtomicInteger(0);

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int evalCount = activeEvaluatorCount.getAndIncrement();

      final String contextId = CONTEXT_ID_PREFIX + evalCount;

      final Configuration partialContextConf = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, contextId)
          .build();

      final Configuration contextConf = Configurations.merge(
          partialContextConf, emConfProvider.getContextConfiguration(), aggregationManager.getContextConfiguration());

      final Configuration serviceConf = Configurations.merge(
          emConfProvider.getServiceConfiguration(contextId, NUM_EVAL),
          aggregationManager.getServiceConfigurationWithoutNameResolver(),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
              .build());

      final Configuration traceConf = traceParameters.getConfiguration();

      final Configuration exampleConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(NumMoves.class, Integer.toString(numMoves))
          .build();

      allocatedEvaluator.submitContextAndService(contextConf,
          Configurations.merge(serviceConf, traceConf, exampleConf));
      LOG.log(Level.INFO, "{0} evaluators active!", evalCount + 1);
    }
  }

  /**
   * Task-0 goes on Context-0, and Task-1 goes on Context-1.
   */
  final class ActiveContextHandler implements EventHandler<ActiveContext> {

    @Override
    public void onNext(final ActiveContext activeContext) {
      final String contextId = activeContext.getId();
      final String taskId = contextId.replace(CONTEXT_ID_PREFIX, TASK_ID_PREFIX);

      final Configuration idFactoryConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class).build();

      // configuration for choosing range or single key implementation of MemoryStore.
      final Configuration rangeTestConf =
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(RangeSupport.class, String.valueOf(rangeSupport))
              .build();

      final Configuration taskConf = Configurations.merge(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, SimpleEMTask.class)
              .build(),
          rangeTestConf,
          idFactoryConf);

      activeContext.submitTask(taskConf);
    }
  }
}
