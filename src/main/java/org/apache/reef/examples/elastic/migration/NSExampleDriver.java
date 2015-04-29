/**
 * Copyright (C) 2014 Seoul National University
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

package org.apache.reef.examples.elastic.migration;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.elastic.memory.utils.*;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerImpl;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for NSExample
 */
@Unit
public final class NSExampleDriver {
  private static final Logger LOG = Logger.getLogger(NSExampleDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final GroupCommDriver groupCommDriver;
  private final CommunicationGroupDriver commGroup;
  private String groupCommControllerId;
  private final AtomicInteger atomicWorkerTaskCount;
  private final AtomicInteger atomicContextCount;
  private final NameServer nameServer;
  private final IdentifierFactory idFac = new StringIdentifierFactory();
  private final String nameServerAddr;
  private final int nameServerPort;
  private final List<String> workerTaskNames;
  private final int workerNum = 2;
  private final AtomicBoolean controllerTaskSubmitted;

  @Inject
  public NSExampleDriver(final EvaluatorRequestor requestor,
                         final GroupCommDriver groupCommDriver) throws InjectionException {
    LOG.log(Level.FINE, "Instantiated Driver");
    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.commGroup = groupCommDriver.newCommunicationGroup(NamedParameters.GroupCommName.class, workerNum + 1);
    this.commGroup.addBroadcast(NamedParameters.Broadcaster.class, BroadcastOperatorSpec.newBuilder()
        .setSenderId("ControllerTask")
        .setDataCodecClass(SerializableCodec.class)
        .build()).finalise();
    this.groupCommControllerId = null;
    this.atomicWorkerTaskCount = new AtomicInteger(0);
    this.atomicContextCount = new AtomicInteger(0);

    this.nameServer = new NameServerImpl(9999, idFac);

    this.nameServerAddr = NetUtils.getLocalAddress();
    this.nameServerPort = nameServer.getPort();
    this.workerTaskNames = new ArrayList<>();
    for (int i = 0; i < workerNum; i++) {
      workerTaskNames.add("WorkerTask" + i);
    }
    this.controllerTaskSubmitted = new AtomicBoolean(false);
  }

  public final class DriverStartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      NSExampleDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(workerNum + 1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int contextCount = atomicContextCount.getAndIncrement();
      final Configuration contextConfiguration = ContextConfiguration.CONF
          .set(ContextConfiguration.IDENTIFIER, "Context" + contextCount)
          .build();
      allocatedEvaluator.submitContext(contextConfiguration);
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {

    private final AtomicBoolean storeControllerId = new AtomicBoolean(false);

    private boolean controllerTaskSubmitted() {
      return !controllerTaskSubmitted.compareAndSet(false, true);
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      if (groupCommDriver.isConfigured(activeContext)) {
        final String contextId = activeContext.getId();
        // makes thread-safe
        if (contextId.equals(groupCommControllerId) && !controllerTaskSubmitted()) {

          LOG.log(Level.INFO, "Submitting ControllerTask to ActiveContext: {0}", activeContext);
          final Configuration partialTaskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, "ControllerTask")
              .set(TaskConfiguration.TASK, ControllerTask.class)
              .build();
          commGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          activeContext.submitTask(taskConf);
        } else {
          // makes thread-safe
          int workerTaskCount = atomicWorkerTaskCount.getAndAdd(1);
          LOG.log(Level.INFO, "Submitting WorkerTask to ActiveContext: {0}", activeContext);
          Configuration basicTaskConf = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, workerTaskNames.get(workerTaskCount))
              .set(TaskConfiguration.TASK, WorkerTask.class)
              .build();
          final JavaConfigurationBuilder taskConfigurationBuilder = Tang.Factory.getTang().newConfigurationBuilder();
          taskConfigurationBuilder.addConfiguration(basicTaskConf);
          taskConfigurationBuilder.bindNamedParameter(WorkerTaskOptions.IndexOfWord.class,
              Integer.toString(workerTaskCount));
          for (int i = 0; i < workerTaskNames.size(); i++) {
            if (workerTaskCount != i)
              taskConfigurationBuilder.bindSetEntry(WorkerTaskOptions.Destinations.class, workerTaskNames.get(i));
          }
          final Configuration partialTaskConf = taskConfigurationBuilder.build();
          commGroup.addTask(partialTaskConf);
          final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
          activeContext.submitTask(taskConf);
        }
      } else {

        final Configuration contextConf = groupCommDriver.getContextConfiguration();
        final Configuration partialServiceConf = groupCommDriver.getServiceConfiguration();

        final Configuration serviceConf;
        // makes thread-safe
        if (storeControllerId.compareAndSet(false, true)) {
          groupCommControllerId = contextId(contextConf);
          serviceConf = partialServiceConf;
        } else {
          // Registers another NetworkService in context by using NSWrapper
          final Configuration nsWrapperServiceConf = ServiceConfiguration.CONF
              .set(ServiceConfiguration.SERVICES, NSWrapper.class)
              .set(ServiceConfiguration.ON_TASK_STARTED, BindNSWrapperToTask.class)
              .set(ServiceConfiguration.ON_TASK_STOP, UnbindNSWrapperFromTask.class)
              .set(ServiceConfiguration.ON_CONTEXT_STOP, NSWrapperClosingHandler.class)
              .build();

          // Parameters needed for NSWrapper to make NetworkService
          final Configuration additionalServiceConf = Tang.Factory.getTang().newConfigurationBuilder(nsWrapperServiceConf)
              .bindNamedParameter(NSWrapperParameters.NameServerAddr.class, nameServerAddr)
              .bindNamedParameter(NSWrapperParameters.NameServerPort.class, Integer.toString(nameServerPort))
              .build();
          serviceConf = Tang.Factory.getTang().newConfigurationBuilder(partialServiceConf,
              additionalServiceConf).build();
        }
        activeContext.submitContextAndService(contextConf, serviceConf);
      }
    }
  }

  public final class ContextCloseHandler implements EventHandler<ClosedContext> {

    @Override
    public void onNext(final ClosedContext closedContext) {
      LOG.log(Level.FINE, "Got closed context: {0}", closedContext.getId());
      final ActiveContext parentContext = closedContext.getParentContext();
      if (parentContext != null) {
        LOG.log(Level.FINE, "Closing parent context: {0}", parentContext.getId());
        parentContext.close();
      }
    }
  }

  private String contextId(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }
}
