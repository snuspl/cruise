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
package edu.snu.cay.services.em.examples.remote;

import edu.snu.cay.common.aggregation.driver.AggregationManager;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.ElasticMemoryConfiguration;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for RemoteEM Example.
 * Two evaluators communicate with each other via remote access feature.
 * Worker tasks are controlled by {@code DriverSideMsgHandler} to check they perform correctly.
 */
@Unit
final class RemoteEMDriver {
  private static final Logger LOG = Logger.getLogger(RemoteEMDriver.class.getName());

  static final String TASK_ID_PREFIX = "Task-";
  static final String AGGREGATION_CLIENT_ID = "AGGREGATION_CLIENT_ID";
  static final String CONTEXT_ID_PREFIX = "Context-";

  static final int EVAL_NUM = 2;

  private final EvaluatorRequestor requestor;

  private final AggregationManager aggregationManager;

  private final ElasticMemoryConfiguration emConf;
  private final HTraceParameters traceParameters;

  private final boolean rangeSupport;

  @Inject
  private RemoteEMDriver(final EvaluatorRequestor requestor,
                         final AggregationManager aggregationManager,
                         final ElasticMemoryConfiguration emConf,
                         final HTraceParameters traceParameters,
                         @Parameter(RangeSupport.class) final boolean rangeSupport) throws InjectionException {
    this.requestor = requestor;
    this.aggregationManager = aggregationManager;
    this.emConf = emConf;
    this.traceParameters = traceParameters;
    this.rangeSupport = rangeSupport;
  }

  /**
   * Spawn two small containers.
   */
  final class DriverStartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(EVAL_NUM)
          // task in this example performs a large amount of remote accesses.
          // since remote operation requires much object creation (routing, getting remote data, encoding/decoding),
          // sometimes JVM fails due to GC overhead (not OOM).
          // I confirmed that the test succeed with 256 Mb in local and 512 Mb in cluster (by Jenkins) respectively.
          // Since it differs for environment, for reliability 1024 Mb has been chosen.
          // TODO #465: minimize GC overhead
          .setMemory(1024)
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
          partialContextConf, emConf.getContextConfiguration(), aggregationManager.getContextConfiguration());

      final Configuration serviceConf = Configurations.merge(
          emConf.getServiceConfiguration(contextId, EVAL_NUM),
          aggregationManager.getServiceConfigurationWithoutNameResolver(),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(EMUpdateFunction.class, EMUpdateFunctionImpl.class)
              .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
              .build());

      final Configuration traceConf = traceParameters.getConfiguration();

      allocatedEvaluator.submitContextAndService(contextConf,
          Configurations.merge(serviceConf, traceConf));
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

      final Configuration idConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class)
          .build();

      // configuration for testing range and single key implementation of MemoryStore.
      final Configuration rangeTestConf =
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(RangeSupport.class, String.valueOf(rangeSupport))
              .build();

      final Configuration taskConf = Configurations.merge(
          TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskId)
              .set(TaskConfiguration.TASK, RemoteEMTask.class)
              .build(),
          rangeTestConf,
          idConf);

      activeContext.submitTask(taskConf);
    }
  }
}
