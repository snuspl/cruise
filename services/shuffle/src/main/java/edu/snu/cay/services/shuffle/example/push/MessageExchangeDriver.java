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

  public static final int ITERATION_NUMBER = 10;
  public static final int NETWORK_MESSAGE_NUMBER_IN_ONE_ITERATION = 3;
  public static final String MESSAGE_EXCHANGE_SHUFFLE_NAME = "MESSAGE_EXCHANGE_SHUFFLE_NAME";
  public static final String SENDER_PREFIX = "SENDER";
  public static final String RECEIVER_PREFIX = "RECEIVER";
  public static final String SENDER_RECEIVER_PREFIX = "SR";

  private final AtomicInteger allocatedNum;
  private final AtomicInteger completedNum;
  private final AtomicInteger totalSentTupleCount;
  private final AtomicInteger totalReceivedTupleCount;
  private final EvaluatorRequestor evaluatorRequestor;
  private final ShuffleDriver shuffleDriver;
  private final LocalAddressProvider localAddressProvider;
  private final NameServer nameServer;

  private final int senderNumber;
  private final int receiverNumber;
  private final int senderReceiverNumber;

  private final List<String> senderIdList;
  private final List<String> receiverIdList;

  @Inject
  private MessageExchangeDriver(
      @Parameter(MessageExchangeREEF.SenderNumber.class) final int senderNumber,
      @Parameter(MessageExchangeREEF.ReceiverNumber.class) final int receiverNumber,
      @Parameter(MessageExchangeREEF.SenderReceiverNumber.class) final int senderReceiverNumber,
      final EvaluatorRequestor evaluatorRequestor,
      final ShuffleDriver shuffleDriver,
      final LocalAddressProvider localAddressProvider,
      final NameServer nameServer) {
    LOG.log(Level.INFO, "The Driver is instantiated. sender num: {0}, receiver num: {1}, sender_receiver num: {2}",
        new Object[]{senderNumber, receiverNumber, senderReceiverNumber});
    this.allocatedNum = new AtomicInteger();
    this.completedNum = new AtomicInteger();
    this.totalSentTupleCount = new AtomicInteger();
    this.totalReceivedTupleCount = new AtomicInteger();
    this.evaluatorRequestor = evaluatorRequestor;
    this.shuffleDriver = shuffleDriver;
    this.localAddressProvider = localAddressProvider;
    this.nameServer = nameServer;
    this.senderIdList = new ArrayList<>(senderNumber + senderReceiverNumber);
    this.receiverIdList = new ArrayList<>(receiverNumber + senderReceiverNumber);
    this.senderNumber = senderNumber;
    this.receiverNumber = receiverNumber;
    this.senderReceiverNumber = senderReceiverNumber;
    for (int i = 0; i < senderNumber; i++) {
      senderIdList.add(SENDER_PREFIX + i);
    }

    for (int i = 0; i < receiverNumber; i++) {
      receiverIdList.add(RECEIVER_PREFIX + i);
    }

    for (int i = 0; i < senderReceiverNumber; i++) {
      senderIdList.add(SENDER_RECEIVER_PREFIX + i);
      receiverIdList.add(SENDER_RECEIVER_PREFIX + i);
    }

    registerShuffle();
  }

  private void registerShuffle() {
    shuffleDriver.registerShuffle(
        ShuffleDescriptionImpl.newBuilder(MESSAGE_EXCHANGE_SHUFFLE_NAME)
            .setSenderIdList(senderIdList)
            .setReceiverIdList(receiverIdList)
            .setKeyCodecClass(IntegerCodec.class)
            .setValueCodecClass(IntegerCodec.class)
            .setShuffleStrategyClass(KeyShuffleStrategy.class)
            .build()
    );
  }

  public final class StartHandler implements EventHandler<StartTime> {

    @Override
    public void onNext(final StartTime value) {
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(senderNumber + receiverNumber + senderReceiverNumber)
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
        final int sentTupleCount = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It sent {1} tuples", new Object[]{taskId, sentTupleCount});
        totalSentTupleCount.addAndGet(sentTupleCount);
      } else if (taskId.startsWith(RECEIVER_PREFIX)) {
        final int receivedTupleCount = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It received {1} tuples", new Object[]{taskId, receivedTupleCount});
        totalReceivedTupleCount.addAndGet(receivedTupleCount);
      } else if (taskId.startsWith(SENDER_RECEIVER_PREFIX)) {
        final int sentTupleCount = byteBuffer.getInt();
        final int receivedTupleCount = byteBuffer.getInt();
        LOG.log(Level.INFO, "{0} completed. It sent {1} tuples and received {2} tuples.",
            new Object[]{taskId, sentTupleCount, receivedTupleCount});
        totalSentTupleCount.addAndGet(sentTupleCount);
        totalReceivedTupleCount.addAndGet(receivedTupleCount);
      } else {
        throw new RuntimeException("Unknown task identifier " + taskId);
      }

      if (completedNum.incrementAndGet() == senderNumber + receiverNumber + senderReceiverNumber) {
        LOG.log(Level.INFO, "Total sent tuple count : {0}, total received tuple count : {1}",
            new Object[]{totalSentTupleCount.get(), totalReceivedTupleCount.get()});
        assert totalSentTupleCount.get() == totalReceivedTupleCount.get();
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

  public final class AllocatedHandler implements EventHandler<AllocatedEvaluator> {

    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final int number = allocatedNum.getAndIncrement();
      final Configuration partialTaskConf;
      final String taskId;
      if (number < senderNumber) { // SenderTask
        taskId = SENDER_PREFIX + number;
        partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, SenderTask.class)
            .build();
      } else if (number < senderNumber + receiverNumber) { // ReceiverTask
        taskId = RECEIVER_PREFIX + (number - senderNumber);
        partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, ReceiverTask.class)
            .build();
      } else if (number < senderNumber + receiverNumber + senderReceiverNumber) { // SenderReceiverTask
        taskId = SENDER_RECEIVER_PREFIX + (number - senderNumber - receiverNumber);
        partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, taskId)
            .set(TaskConfiguration.TASK, SenderReceiverTask.class)
            .build();
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
