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

package edu.snu.reef.em.examples.elastic.migration;

import edu.snu.reef.em.driver.ElasticMemoryDriverConfiguration;
import edu.snu.reef.em.msg.ElasticMemoryCtrlMsg;
import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgCodec;
import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgHandler;
import edu.snu.reef.em.driver.ContextMsgSender;
import edu.snu.reef.em.examples.parameters.DataBroadcast;
import edu.snu.reef.em.examples.parameters.CommGroupName;
import edu.snu.reef.em.examples.parameters.WorkerTaskOptions;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.evaluator.context.parameters.ContextMessageHandlers;
import org.apache.reef.io.network.group.api.driver.CommunicationGroupDriver;
import org.apache.reef.io.network.group.api.driver.GroupCommDriver;
import org.apache.reef.io.network.group.impl.config.BroadcastOperatorSpec;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

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

  private final AtomicInteger cmpTaskCount;
  private final String nameServerAddr;
  private final int nameServerPort;
  private final List<String> cmpTaskNames;
  private final int workerNum;
  private final AtomicBoolean controllerTaskSubmitted;

  private final ContextMsgSender contextMsgSender;
  private final ReadyCodec readyCodec;
  private final AtomicInteger notReadyTasks;
  private final ElasticMemoryCtrlMsgCodec msgCodec;

  private final ElasticMemoryDriverConfiguration emDriverConf;

  @Inject
  public NSExampleDriver(final EvaluatorRequestor requestor,
                         final GroupCommDriver groupCommDriver,
                         final NameServer nameServer,
                         final LocalAddressProvider localAddressProvider,
                         final ContextMsgSender contextMsgSender,
                         final ReadyCodec readyCodec,
                         final ElasticMemoryCtrlMsgCodec msgCodec,
                         final ElasticMemoryDriverConfiguration emDriverConf) throws InjectionException {
    this.contextMsgSender = contextMsgSender;
//    System.out.println(contextMsgSender);
    this.readyCodec = readyCodec;
    this.msgCodec = msgCodec;
    this.emDriverConf = emDriverConf;

    // TODO: fix
    this.workerNum = 2;

    this.notReadyTasks = new AtomicInteger(this.workerNum);

    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.commGroup = groupCommDriver.newCommunicationGroup(CommGroupName.class, workerNum + 1);
    this.commGroup
        .addBroadcast(DataBroadcast.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(CtrlTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .finalise();

    this.nameServerAddr = localAddressProvider.getLocalAddress();
    this.nameServerPort = nameServer.getPort();

    this.cmpTaskCount = new AtomicInteger(0);
    this.cmpTaskNames = new ArrayList<>();
    for (int i = 0; i < workerNum; i++) {
      cmpTaskNames.add(CmpTask.TASK_ID_PREFIX + i);
    }
    this.controllerTaskSubmitted = new AtomicBoolean(false);
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
//      System.out.println(contextMsgSender);
    }
  }

  public final class DriverStartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      NSExampleDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(workerNum + 1)
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    /**
     * atomic object for thread safeness
     */
    private final AtomicBoolean isCtrlEvaluatorSelected = new AtomicBoolean(false);

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      // Submit context for Group Communication and an additional Network Service
      final Configuration partialContextConf = groupCommDriver.getContextConfiguration();
      final Configuration finalContextConf;
      final Configuration partialServiceConf = groupCommDriver.getServiceConfiguration();
      final Configuration finalServiceConf;

      if (isCtrlEvaluatorSelected.compareAndSet(false, true)) {
        // I will be the Control Evaluator if no one already is
        LOG.log(Level.INFO, "Submitting Ctrl context to AllocatedEvaluator: {0}", allocatedEvaluator);

        groupCommControllerId = contextId(partialContextConf);
        finalContextConf = partialContextConf;
        finalServiceConf = partialServiceConf;

      } else {
        // I will be a Compute Evaluator
        LOG.log(Level.INFO, "Submitting Cmp context to AllocatedEvaluator: {0}", allocatedEvaluator);

        finalContextConf = Configurations.merge(partialContextConf, emDriverConf.getContextConfiguration());
        finalServiceConf = Configurations.merge(partialServiceConf, emDriverConf.getServiceConfiguration());
      }

      allocatedEvaluator.submitContextAndService(finalContextConf, finalServiceConf);
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    private final boolean controllerTaskSubmitted() {
      return !controllerTaskSubmitted.compareAndSet(false, true);
    }

    @Override
    public void onNext(final ActiveContext activeContext) {
      // Submit either CtrlTask or CmpTask
      // thread-safe check
      if (activeContext.getId().equals(groupCommControllerId) && !controllerTaskSubmitted()) {
        // I am the Control Evaluator. Let's submit CtrlTask.
        LOG.log(Level.INFO, "Submitting CtrlTask to ActiveContext: {0}", activeContext);

        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, CtrlTask.TASK_ID)
            .set(TaskConfiguration.TASK, CtrlTask.class)
            .build();
        commGroup.addTask(partialTaskConf);
        activeContext.submitTask(groupCommDriver.getTaskConfiguration(partialTaskConf));

      } else {
        // I am a Compute Evaluator. Let's submit CmpTask.
        final int slaveTaskIndex = cmpTaskCount.getAndIncrement();
        LOG.log(Level.INFO, "Submitting CmpTask-{0} to ActiveContext: {1}",
            new Object[] {slaveTaskIndex, activeContext});

        final Configuration basicTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, CmpTask.TASK_ID_PREFIX + slaveTaskIndex)
            .set(TaskConfiguration.TASK, CmpTask.class)
            .set(TaskConfiguration.ON_SEND_MESSAGE, CmpTaskReady.class)
            .build();
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder(basicTaskConf);
//        jcb.bindImplementation(MemoryStoreClient.class, ElasticMemoryServiceClient.class);
        for (int i = 0; i < cmpTaskNames.size(); i++) {
          if (slaveTaskIndex != i)
            jcb.bindSetEntry(WorkerTaskOptions.Destinations.class, cmpTaskNames.get(i));
        }
        final Configuration partialTaskConf = jcb.build();
        commGroup.addTask(partialTaskConf);
        activeContext.submitTask(groupCommDriver.getTaskConfiguration(partialTaskConf));
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

  public final class TaskMessageHandler implements EventHandler<TaskMessage> {
    private String prevTaskId = "DEFAULT";

    @Override
    public void onNext(final TaskMessage taskMessage) {
      System.out.println("Received task message.");
      System.out.println(taskMessage.getContextId());
      final Boolean result = readyCodec.decode(taskMessage.get());
      System.out.println(result);
      if (result && notReadyTasks.decrementAndGet() == 0) {
        System.out.println("READY!!");
        contextMsgSender.send(taskMessage.getContextId(),
            msgCodec.encode(new ElasticMemoryCtrlMsg("String", prevTaskId)));
      }
      prevTaskId = taskMessage.getId();
      System.out.println();
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
