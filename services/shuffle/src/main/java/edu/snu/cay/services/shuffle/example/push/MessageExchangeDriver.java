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
import edu.snu.cay.services.shuffle.evaluator.ShuffleContextStopHandler;
import edu.snu.cay.services.shuffle.strategy.KeyShuffleStrategy;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.Services;
import org.apache.reef.io.network.impl.NetworkConnectionServiceImpl;
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
  public static final String SENDER_AND_RECEIVER_PREFIX = "SEND_AND_RECEIVER";

  private static final String TASK_POSTFIX = "-TASK";

  private final AtomicInteger numAllocatedEvaluators;
  private final AtomicInteger numCompletedTasks;
  private final AtomicInteger totalNumSentTuples;
  private final AtomicInteger totalNumReceivedTuples;
  private final EvaluatorRequestor evaluatorRequestor;
  private final StaticPushShuffleManager shuffleManager;
  private final LocalAddressProvider localAddressProvider;
  private final NameServer nameServer;

  private final int totalNumOnlySenders;
  private final int totalNumOnlyReceivers;
  private final int totalNumSendersAndReceivers;
  private final int numTotalIterations;

  @Inject
  private MessageExchangeDriver(
      @Parameter(MessageExchangeParameters.SenderNumber.class) final int onlySenderNumber,
      @Parameter(MessageExchangeParameters.ReceiverNumber.class) final int onlyReceiverNumber,
      @Parameter(MessageExchangeParameters.SenderAndReceiverNumber.class) final int senderAndReceiverNumber,
      final EvaluatorRequestor evaluatorRequestor,
      final ShuffleDriver shuffleDriver,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer,
      @Parameter(MessageExchangeParameters.Shutdown.class) final boolean shutdown,
      @Parameter(MessageExchangeParameters.TotalIterationNum.class) final int numTotalIterations,
      @Parameter(MessageExchangeParameters.ShutdownIterationNum.class) final int numShutdownIterations) {
    LOG.log(Level.INFO, "The Driver is instantiated. sender num: {0}, receiver num: {1}, sender and receiver num : {2}",
        new Object[]{onlySenderNumber, onlyReceiverNumber, senderAndReceiverNumber});
    this.numAllocatedEvaluators = new AtomicInteger();
    this.numCompletedTasks = new AtomicInteger();
    this.totalNumSentTuples = new AtomicInteger();
    this.totalNumReceivedTuples = new AtomicInteger();
    this.evaluatorRequestor = evaluatorRequestor;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    this.totalNumOnlySenders = onlySenderNumber;
    this.totalNumOnlyReceivers = onlyReceiverNumber;
    this.totalNumSendersAndReceivers = senderAndReceiverNumber;
    this.numTotalIterations = numTotalIterations;
    final List<String> senderIdList = new ArrayList<>(onlySenderNumber + senderAndReceiverNumber);
    final List<String> receiverIdList = new ArrayList<>(onlyReceiverNumber + senderAndReceiverNumber);

    for (int i = 0; i < onlySenderNumber; i++) {
      senderIdList.add(SENDER_PREFIX + i);
    }

    for (int i = 0; i < onlyReceiverNumber; i++) {
      receiverIdList.add(RECEIVER_PREFIX + i);
    }

    for (int i = 0; i < senderAndReceiverNumber; i++) {
      senderIdList.add(SENDER_AND_RECEIVER_PREFIX + i);
      receiverIdList.add(SENDER_AND_RECEIVER_PREFIX + i);
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
          .setNumber(totalNumOnlySenders + totalNumOnlyReceivers + totalNumSendersAndReceivers)
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

      } else if (taskId.startsWith(SENDER_AND_RECEIVER_PREFIX)) {
        final int numSentTuples = byteBuffer.getInt();
        final int numReceivedTuples = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It sent {1} tuples and received {2} tuples",
            new Object[]{taskId, numSentTuples, numReceivedTuples});
        totalNumSentTuples.addAndGet(numSentTuples);
        totalNumReceivedTuples.addAndGet(numReceivedTuples);

      } else {
        throw new RuntimeException("Unknown task identifier " + taskId);
      }

      if (numCompletedTasks.incrementAndGet() ==
          totalNumOnlySenders + totalNumOnlyReceivers + totalNumSendersAndReceivers) {
        LOG.log(Level.INFO, "Total sent tuple number : {0}, total received tuple number : {1}",
            new Object[]{totalNumSentTuples.get(), totalNumReceivedTuples.get()});
        if (totalNumSentTuples.get() != totalNumReceivedTuples.get()) {
          throw new RuntimeException("Total sent tuple number " + totalNumSentTuples.get() + " have to be same as "
              + " total received tuple number " + totalNumReceivedTuples.get());
        }
      }
    }
  }

  private Configuration getContextConfiguration(final String endPointId) {
    final Configuration partialContextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "MessageExchangeContext")
        .set(ContextConfiguration.ON_CONTEXT_STOP, ShuffleContextStopHandler.class)
        .build();

    return Configurations.merge(partialContextConf, shuffleManager.getShuffleConfiguration(endPointId));
  }

  private Configuration getServiceConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(Services.class, NetworkConnectionServiceImpl.class)
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindNamedParameter(NameResolverNameServerPort.class, String.valueOf(nameServer.getPort()))
        .build();
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int number = numAllocatedEvaluators.getAndIncrement();
      final Configuration taskConf;
      final String taskId;
      final String endPointId;
      final Configuration iterationConf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindNamedParameter(MessageExchangeParameters.TotalIterationNum.class, Integer.toString(numTotalIterations))
          .build();
      if (number < totalNumOnlySenders) { // SenderTask
        endPointId = SENDER_PREFIX + number;
        taskId = endPointId + TASK_POSTFIX;
        taskConf = Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, SenderTask.class)
            .build(), iterationConf);
      } else if (number < totalNumOnlySenders + totalNumOnlyReceivers) { // ReceiverTask
        endPointId = RECEIVER_PREFIX + (number - totalNumOnlySenders);
        taskId = endPointId + TASK_POSTFIX;
        taskConf = Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, ReceiverTask.class)
            .build(), iterationConf);
      } else if (number < totalNumOnlySenders + totalNumOnlyReceivers + totalNumSendersAndReceivers) {
        // SenderAndReceiverTask
        endPointId = SENDER_AND_RECEIVER_PREFIX + (number - totalNumOnlySenders - totalNumOnlyReceivers);
        taskId = endPointId + TASK_POSTFIX;
        taskConf = Configurations.merge(TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, SenderAndReceiverTask.class)
            .build(), iterationConf);
      } else {
        throw new RuntimeException("Too many allocated evaluators");
      }

      allocatedEvaluator.submitContextAndServiceAndTask(
          getContextConfiguration(endPointId),
          getServiceConfiguration(),
          taskConf
      );
    }
  }
}
