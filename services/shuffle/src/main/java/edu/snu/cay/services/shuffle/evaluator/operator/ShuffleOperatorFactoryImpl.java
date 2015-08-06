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
package edu.snu.cay.services.shuffle.evaluator.operator;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.ShuffleTupleMessageCodec;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import edu.snu.cay.services.shuffle.evaluator.TupleCodec;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Default implementation of ShuffleOperatorFactory.
 */
final class ShuffleOperatorFactoryImpl<K, V> implements ShuffleOperatorFactory<K, V> {

  // TODO: currently the identifier of the end point is same as the task identifier.
  // It have to be changed for the case Shuffles are injected in context configuration.
  private final String endPointId;
  private final ShuffleDescription shuffleDescription;
  private final ShuffleTupleMessageCodec shuffleTupleCodec;
  private final Injector injector;

  private ShuffleSender shuffleSender;
  private ShuffleReceiver shuffleReceiver;

  @Inject
  private ShuffleOperatorFactoryImpl(
      @Parameter(TaskConfigurationOptions.Identifier.class) final String endPointId,
      final ShuffleDescription shuffleDescription,
      final ShuffleTupleMessageCodec shuffleTupleCodec,
      final Injector injector) {
    this.endPointId = endPointId;
    this.shuffleDescription = shuffleDescription;
    this.shuffleTupleCodec = shuffleTupleCodec;
    this.injector = injector;
  }

  private void addTupleCodec(final ShuffleDescription shuffleDescription) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final Configuration tupleCodecConf = confBuilder
        .bindNamedParameter(ShuffleParameters.TupleKeyCodec.class, shuffleDescription.getKeyCodecClass())
        .bindNamedParameter(ShuffleParameters.TupleValueCodec.class, shuffleDescription.getValueCodecClass())
        .build();
    try {
      final Codec<Tuple> tupleCodec = Tang.Factory.getTang().newInjector(tupleCodecConf).getInstance(TupleCodec.class);
      shuffleTupleCodec.registerTupleCodec(shuffleDescription.getShuffleName(), tupleCodec);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T extends ShuffleReceiver<K, V>> T newShuffleReceiver() {
    final String shuffleName = shuffleDescription.getShuffleName();
    if (shuffleReceiver == null) {
      if (!shuffleDescription.getReceiverIdList().contains(endPointId)) {
        throw new RuntimeException(shuffleName + " does not have " + endPointId + " as a receiver.");
      }

      final Injector forkedInjector = getForkedInjectorWithParameters(shuffleDescription);

      try {
        shuffleReceiver = forkedInjector.getInstance(ShuffleReceiver.class);
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An Exception occurred while injecting receiver with " + shuffleDescription, e);
      }
    }

    return (T)shuffleReceiver;
  }

  @Override
  public <T extends ShuffleSender<K, V>> T newShuffleSender() {
    final String shuffleName = shuffleDescription.getShuffleName();
    if (shuffleSender == null) {
      if (!shuffleDescription.getSenderIdList().contains(endPointId)) {
        throw new RuntimeException(shuffleName + " does not have " + endPointId + " as a sender.");
      }

      final Injector forkedInjector = getForkedInjectorWithParameters(shuffleDescription);

      try {
        shuffleSender = forkedInjector.getInstance(ShuffleSender.class);
        addTupleCodec(shuffleDescription);
      } catch (final InjectionException e) {
        throw new RuntimeException("An InjectionException occurred while injecting sender with " +
            shuffleDescription, e);
      }
    }

    return (T)shuffleSender;
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
