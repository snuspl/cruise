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
package edu.snu.cay.services.shuffle.description;

import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Default implementation of ShuffleDescription, it can be instantiated using
 * Builder or Tang injector. Builder is used in driver when a user register
 * a shuffle description to ShuffleDriver.
 * Tang injector is used in an evaluator when the serialized shuffle description
 * for the evaluator are deserialized by Tang injection.
 */
public final class ShuffleDescriptionImpl implements ShuffleDescription {

  private final String shuffleName;
  private final List<String> senderIdList;
  private final List<String> receiverIdList;
  private final Class<? extends ShuffleStrategy> shuffleStrategyClass;
  private final Class<? extends Codec> keyCodecClass;
  private final Class<? extends Codec> valueCodecClass;

  @Inject
  private ShuffleDescriptionImpl(
      @Parameter(ShuffleParameters.ShuffleName.class) final String shuffleName,
      @Parameter(ShuffleParameters.ShuffleSenderIdSet.class) final Set<String> senderIdSet,
      @Parameter(ShuffleParameters.ShuffleReceiverIdSet.class) final Set<String> receiverIdSet,
      @Parameter(ShuffleParameters.ShuffleStrategyClassName.class) final String shuffleStrategyClassName,
      @Parameter(ShuffleParameters.ShuffleKeyCodecClassName.class) final String keyCodecClassName,
      @Parameter(ShuffleParameters.ShuffleValueCodecClassName.class) final String valueCodecClassName) {
    this.shuffleName = shuffleName;
    this.senderIdList = new ArrayList<>(senderIdSet);
    this.receiverIdList = new ArrayList<>(receiverIdSet);
    // Set does not guarantee that the order of elements is preserved, we should at least sort the receiver ids.
    Collections.sort(senderIdList);
    Collections.sort(receiverIdList);

    try {
      shuffleStrategyClass = (Class<? extends ShuffleStrategy>) Class.forName(shuffleStrategyClassName);
      keyCodecClass = (Class<? extends Codec>) Class.forName(keyCodecClassName);
      valueCodecClass = (Class<? extends Codec>) Class.forName(valueCodecClassName);
    } catch (final ClassNotFoundException exception) {
      throw new RuntimeException("ClassNotFoundException occurred in constructor of ShuffleDescriptionImpl", exception);
    }
  }

  private ShuffleDescriptionImpl(
      final String shuffleName,
      final List<String> senderIdList,
      final List<String> receiverIdList,
      final Class<? extends ShuffleStrategy> shuffleStrategyClass,
      final Class<? extends Codec> keyCodecClass,
      final Class<? extends Codec> valueCodecClass) {
    this.shuffleName = shuffleName;
    this.senderIdList = senderIdList;
    this.receiverIdList = receiverIdList;
    Collections.sort(senderIdList);
    Collections.sort(receiverIdList);
    this.shuffleStrategyClass = shuffleStrategyClass;
    this.keyCodecClass = keyCodecClass;
    this.valueCodecClass = valueCodecClass;
  }

  @Override
  public String getShuffleName() {
    return shuffleName;
  }

  @Override
  public Class<? extends ShuffleStrategy> getShuffleStrategyClass() {
    return shuffleStrategyClass;
  }

  @Override
  public Class<? extends Codec> getKeyCodecClass() {
    return keyCodecClass;
  }

  @Override
  public Class<? extends Codec> getValueCodecClass() {
    return valueCodecClass;
  }

  @Override
  public List<String> getSenderIdList() {
    return senderIdList;
  }

  @Override
  public List<String> getReceiverIdList() {
    return receiverIdList;
  }

  /**
   * @param shuffleName the name of shuffle
   * @return a builder for a ShuffleDescriptionImpl
   */
  public static Builder newBuilder(final String shuffleName) {
    return new Builder(shuffleName);
  }

  /**
   * Builder to construct a ShuffleDescriptionImpl instance.
   */
  public static final class Builder {
    private final String shuffleName;
    private Class<? extends ShuffleStrategy> shuffleStrategyClass;
    private Class<? extends Codec> keyCodecClass;
    private Class<? extends Codec> valueCodecClass;
    private List<String> senderIdList;
    private List<String> receiverIdList;

    private Builder(final String shuffleName) {
      this.shuffleName = shuffleName;
    }

    /**
     * @param senderIdList a sender id list
     * @return the builder itself
     */
    public Builder setSenderIdList(final List<String> senderIdList) {
      this.senderIdList = senderIdList;
      return this;
    }

    /**
     * @param receiverIdList a receiver id list
     * @return the builder itself
     */
    public Builder setReceiverIdList(final List<String> receiverIdList) {
      this.receiverIdList = receiverIdList;
      return this;
    }

    /**
     * @param keyCodecClass a key codec class
     * @return the builder itself
     */
    public Builder setKeyCodec(final Class<? extends Codec> keyCodecClass) {
      this.keyCodecClass = keyCodecClass;
      return this;
    }

    /**
     * @param valueCodecClass a value codec class
     * @return the builder itself
     */
    public Builder setValueCodec(final Class<? extends Codec> valueCodecClass) {
      this.valueCodecClass = valueCodecClass;
      return this;
    }

    /**
     * @param shuffleStrategyClass a shuffle strategy class
     * @return the builder itself
     */
    public Builder setShuffleStrategy(final Class<? extends ShuffleStrategy> shuffleStrategyClass) {
      this.shuffleStrategyClass = shuffleStrategyClass;
      return this;
    }

    /**
     * Build a ShuffleDescriptionImpl instance using set parameters.
     * It throws a RuntimeException if some parameters are omitted.
     *
     * @return a ShuffleDescriptionImpl instance that is created by the builder
     */
    public ShuffleDescription build() {
      if (shuffleStrategyClass == null) {
        throw new RuntimeException("You should set strategy class");
      }

      if (keyCodecClass == null || valueCodecClass == null) {
        throw new RuntimeException("You should set codec for both key and value type");
      }

      return new ShuffleDescriptionImpl(
          shuffleName, senderIdList, receiverIdList, shuffleStrategyClass, keyCodecClass, valueCodecClass);
    }
  }
}
