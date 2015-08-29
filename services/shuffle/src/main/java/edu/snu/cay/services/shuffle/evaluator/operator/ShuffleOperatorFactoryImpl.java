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

  // TODO #63: currently the identifier of the end point is same as the task identifier.
  // It have to be changed for the case Shuffles are injected in context configuration.
  private final String endPointId;
  private final ShuffleDescription shuffleDescription;
  private final ShuffleTupleMessageCodec shuffleTupleCodec;
  private final Injector rootInjector;

  /**
   * Construct a factory that creates shuffle operators.
   * This should be instantiated once for each shuffle, using several forked injectors.
   *
   * @param endPointId the end point id
   * @param shuffleDescription the description of the corresponding shuffle
   * @param shuffleTupleCodec the codec for shuffle tuple messages
   * @param rootInjector the root injector to share components that are already created
   */
  @Inject
  private ShuffleOperatorFactoryImpl(
      @Parameter(TaskConfigurationOptions.Identifier.class) final String endPointId,
      final ShuffleDescription shuffleDescription,
      final ShuffleTupleMessageCodec shuffleTupleCodec,
      final Injector rootInjector) {
    this.endPointId = endPointId;
    this.shuffleDescription = shuffleDescription;
    this.shuffleTupleCodec = shuffleTupleCodec;
    this.rootInjector = rootInjector;
    addTupleCodec();
  }

  private void addTupleCodec() {
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
  public ShuffleReceiver<K, V> newShuffleReceiver() {

    if (!shuffleDescription.getReceiverIdList().contains(endPointId)) {
      return null;
    }

    final Injector forkedInjector = getForkedInjectorWithParameters();
    try {
      return forkedInjector.getInstance(ShuffleReceiver.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("An Exception occurred while injecting receiver with " + shuffleDescription, e);
    }
  }

  @Override
  public ShuffleSender<K, V> newShuffleSender() {
    if (!shuffleDescription.getSenderIdList().contains(endPointId)) {
      return null;
    }

    final Injector forkedInjector = getForkedInjectorWithParameters();
    try {
      return forkedInjector.getInstance(ShuffleSender.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while injecting sender with " +
          shuffleDescription, e);
    }
  }

  private Injector getForkedInjectorWithParameters() {
    final Injector forkedInjector = rootInjector.forkInjector(getBaseOperatorConfiguration());
    forkedInjector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
    return forkedInjector;
  }

  private Configuration getBaseOperatorConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(ShuffleStrategy.class, shuffleDescription.getShuffleStrategyClass())
        .build();
  }
}
