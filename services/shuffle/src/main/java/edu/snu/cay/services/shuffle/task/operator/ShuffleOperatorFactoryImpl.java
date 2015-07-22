/**
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
package edu.snu.cay.services.shuffle.task.operator;

import edu.snu.cay.services.shuffle.description.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.GlobalTupleMessageCodec;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import edu.snu.cay.services.shuffle.task.TupleCodec;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of TupleOperatorFactory.
 */
public class ShuffleOperatorFactoryImpl implements ShuffleOperatorFactory {

  private final String currentTaskId;
  private final String shuffleGroupName;
  private final GlobalTupleMessageCodec globalTupleCodec;
  private final Injector injector;

  private Map<String, ShuffleSender> senderMap;
  private Map<String, ShuffleReceiver> receiverMap;

  @Inject
  private ShuffleOperatorFactoryImpl(
      @Parameter(TaskConfigurationOptions.Identifier.class) final String currentTaskId,
      @Parameter(ShuffleParameters.ShuffleGroupName.class) final String shuffleGroupName,
      final GlobalTupleMessageCodec globalTupleCodec,
      final Injector injector) {
    this.currentTaskId = currentTaskId;
    this.shuffleGroupName = shuffleGroupName;
    this.globalTupleCodec = globalTupleCodec;
    this.injector = injector;
    this.senderMap = new ConcurrentHashMap<>();
    this.receiverMap = new ConcurrentHashMap<>();
  }

  private void addTupleCodec(final ShuffleDescription shuffleDescription) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final Configuration tupleCodecConf = confBuilder
        .bindNamedParameter(ShuffleParameters.ShuffleKeyCodec.class, shuffleDescription.getKeyCodecClass())
        .bindNamedParameter(ShuffleParameters.ShuffleValueCodec.class, shuffleDescription.getValueCodecClass())
        .build();
    try {
      final Codec<Tuple> tupleCodec = Tang.Factory.getTang().newInjector(tupleCodecConf).getInstance(TupleCodec.class);
      globalTupleCodec.registerTupleCodec(shuffleGroupName, shuffleDescription.getShuffleName(), tupleCodec);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K, V> ShuffleReceiver<K, V> newShuffleReceiver(final ShuffleDescription shuffleDescription) {
    final String shuffleName = shuffleDescription.getShuffleName();
    if (!receiverMap.containsKey(shuffleName)) {
      if (!shuffleDescription.getReceiverIdList().contains(currentTaskId)) {
        throw new RuntimeException(shuffleName + " does not have " + currentTaskId + " as a receiver.");
      }

      final Injector forkedInjector = getForkedInjectorWithParameters(shuffleDescription);

      try {
        receiverMap.put(shuffleName, forkedInjector.getInstance(ShuffleReceiver.class));
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An Exception occurred while injecting receiver with " + shuffleDescription, e);
      }
    }

    return receiverMap.get(shuffleName);
  }

  @Override
  public <K, V> ShuffleSender<K, V> newShuffleSender(final ShuffleDescription shuffleDescription) {
    final String shuffleName = shuffleDescription.getShuffleName();
    if (!senderMap.containsKey(shuffleName)) {
      if (!shuffleDescription.getSenderIdList().contains(currentTaskId)) {
        throw new RuntimeException(shuffleName + " does not have " + currentTaskId + " as a sender.");
      }

      final Injector forkedInjector = getForkedInjectorWithParameters(shuffleDescription);

      try {
        senderMap.put(shuffleName, forkedInjector.getInstance(ShuffleSender.class));
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting sender with " +
            shuffleDescription, e);
      }
    }

    return senderMap.get(shuffleName);
  }

  private Injector getForkedInjectorWithParameters(final ShuffleDescription shuffleDescription) {
    final Injector forkedInjector = injector.forkInjector(getBaseOperatorConfiguration(shuffleDescription));
    forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
    return forkedInjector;
  }

  private Configuration getBaseOperatorConfiguration(final ShuffleDescription shuffleDescription) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ShuffleStrategy.class, shuffleDescription.getShuffleStrategyClass())
        .build();
  }
}
