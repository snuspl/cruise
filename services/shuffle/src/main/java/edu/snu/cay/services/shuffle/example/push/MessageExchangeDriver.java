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
package edu.snu.cay.services.shuffle.example.push;

import edu.snu.cay.services.shuffle.common.ShuffleDescriptionImpl;
import edu.snu.cay.services.shuffle.driver.ShuffleDriver;
import edu.snu.cay.services.shuffle.driver.impl.StaticPushShuffleManager;
import edu.snu.cay.services.shuffle.strategy.KeyShuffleStrategy;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF driver for message exchanging example using push-based shuffle.
 */
@DriverSide
@Unit
public final class MessageExchangeDriver {

  private static final Logger LOG = Logger.getLogger(MessageExchangeDriver.class.getName());

  public static final int NETWORK_MESSAGE_NUMBER_IN_ONE_ITERATION = 3;
  public static final String MESSAGE_EXCHANGE_SHUFFLE_NAME = "MESSAGE_EXCHANGE_SHUFFLE_NAME";
  public static final String SENDER_PREFIX = "SENDER";
  public static final String RECEIVER_PREFIX = "RECEIVER";

  private final AtomicInteger numAllocatedEvaluators;
  private final AtomicInteger numCompletedTasks;
  private final AtomicInteger totalNumSentTuples;
  private final AtomicInteger totalNumReceivedTuples;
  private final EvaluatorRequestor evaluatorRequestor;
  private final ShuffleDriver shuffleDriver;
  private final StaticPushShuffleManager shuffleManager;
  private final LocalAddressProvider localAddressProvider;
  private final NameServer nameServer;

  private final int totalNumSenders;
  private final int totalNumReceivers;
  private final int numTotalIterations;

  @Inject
  private MessageExchangeDriver(
      @Parameter(MessageExchangeParameters.SenderNumber.class) final int senderNumber,
      @Parameter(MessageExchangeParameters.ReceiverNumber.class) final int receiverNumber,
      final EvaluatorRequestor evaluatorRequestor,
      final ShuffleDriver shuffleDriver,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer,
      @Parameter(MessageExchangeParameters.Shutdown.class) final boolean shutdown,
      @Parameter(MessageExchangeParameters.TotalIterationNum.class) final int numTotalIterations,
      @Parameter(MessageExchangeParameters.ShutdownIterationNum.class) final int numShutdownIterations) {
    LOG.log(Level.INFO, "The Driver is instantiated. sender num: {0}, receiver num: {1}",
        new Object[]{senderNumber, receiverNumber});
    this.numAllocatedEvaluators = new AtomicInteger();
    this.numCompletedTasks = new AtomicInteger();
    this.totalNumSentTuples = new AtomicInteger();
    this.totalNumReceivedTuples = new AtomicInteger();
    this.evaluatorRequestor = evaluatorRequestor;
    this.shuffleDriver = shuffleDriver;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    this.totalNumSenders = senderNumber;
    this.totalNumReceivers = receiverNumber;
    this.numTotalIterations = numTotalIterations;
    final List<String> senderIdList = new ArrayList<>(senderNumber);
    final List<String> receiverIdList = new ArrayList<>(receiverNumber);

    for (int i = 0; i < senderNumber; i++) {
      senderIdList.add(SENDER_PREFIX + i);
    }

    for (int i = 0; i < receiverNumber; i++) {
      receiverIdList.add(RECEIVER_PREFIX + i);
    }

    this.shuffleManager = shuffleDriver.registerShuffle(
        ShuffleDescriptionImpl.newBuilder(MESSAGE_EXCHANGE_SHUFFLE_NAME)
            .setSenderIdList(senderIdList)
            .setReceiverIdList(receiverIdList)
            .setKeyCodecClass(IntegerCodec.class)
            .setValueCodecClass(IntegerCodec.class)
            .setShuffleStrategyClass(KeyShuffleStrategy.class)
            .build()
    );

    this.shuffleManager.setPushShuffleListener(
        new SimplePushShuffleListener(shuffleManager, shutdown, numShutdownIterations));
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(totalNumSenders + totalNumReceivers)
          .setMemory(128)
          .setNumberOfCores(1)
          .build());
    }
  }

  /**
   * Compare total number of sent tuples and received tuples.
   */
  public final class TaskCompletedHandler implements EventHandler<CompletedTask> {

    @Override
    public void onNext(final CompletedTask completedTask) {
      completedTask.getActiveContext().close();
      final String taskId = completedTask.getId();
      final ByteBuffer byteBuffer = ByteBuffer.wrap(completedTask.get());
      if (taskId.startsWith(SENDER_PREFIX)) {
        final int numSentTuples = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It sent {1} tuples", new Object[]{taskId, numSentTuples});
        totalNumSentTuples.addAndGet(numSentTuples);

      } else if (taskId.startsWith(RECEIVER_PREFIX)) {
        final int numReceivedTuples = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It received {1} tuples", new Object[]{taskId, numReceivedTuples});
        totalNumReceivedTuples.addAndGet(numReceivedTuples);

      } else {
        throw new RuntimeException("Unknown task identifier " + taskId);
      }

      if (numCompletedTasks.incrementAndGet() == totalNumSenders + totalNumReceivers) {
        LOG.log(Level.INFO, "Total sent tuple number : {0}, total received tuple number : {1}",
            new Object[]{totalNumSentTuples.get(), totalNumReceivedTuples.get()});
        if (totalNumSentTuples.get() != totalNumReceivedTuples.get()) {
          throw new RuntimeException("Total sent tuple number " + totalNumSentTuples.get() + " have to be same as "
              + " total received tuple number " + totalNumReceivedTuples.get());
        }
      }
    }
  }

  private Configuration getContextConfiguration() {
    final Configuration partialContextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "MessageExchangeContext")
        .build();

    return Configurations.merge(partialContextConf, shuffleDriver.getContextConfiguration());
  }

  private Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServer.getPort()))
        .build();
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int number = numAllocatedEvaluators.getAndIncrement();
      final Configuration partialTaskConf;
      final String taskId;
      final Configuration iterationConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(MessageExchangeParameters.TotalIterationNum.class, Integer.toString(numTotalIterations))
          .build();

      if (number < totalNumSenders) { // SenderTask
        taskId = SENDER_PREFIX + number;
        partialTaskConf = Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, SenderTask.class)
            .build(), iterationConf);

      } else if (number < totalNumSenders + totalNumReceivers) { // ReceiverTask
        taskId = RECEIVER_PREFIX + (number - totalNumSenders);
        partialTaskConf = Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, ReceiverTask.class)
            .build(), iterationConf);

      } else {
        throw new RuntimeException("Too many allocated evaluators");
      }

      allocatedEvaluator.submitContextAndServiceAndTask(
          getContextConfiguration(),
          getServiceConfiguration(),
          Configurations.merge(partialTaskConf, shuffleDriver.getTaskConfiguration(taskId)));
    }
  }
}
