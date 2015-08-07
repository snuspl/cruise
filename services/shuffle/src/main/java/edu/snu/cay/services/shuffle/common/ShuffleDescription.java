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
package edu.snu.cay.services.shuffle.common;

import edu.snu.cay.services.shuffle.strategy.ShuffleStrategy;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.Codec;

import java.util.List;

/**
 * Description of a shuffle containing key, value, shuffle strategy classes
 * and sender, receiver identifiers.
 */
@DefaultImplementation(ShuffleDescriptionImpl.class)
public interface ShuffleDescription {

  /**
   * @return name of the shuffle
   */
  String getShuffleName();

  /**
   * @return strategy class of the shuffle
   */
  Class<? extends ShuffleStrategy> getShuffleStrategyClass();

  /**
   * @return codec class for tuple keys
   */
  Class<? extends Codec> getKeyCodecClass();

  /**
   * @return codec class for tuple values
   */
  Class<? extends Codec> getValueCodecClass();

  /**
   * @return list of sender identifiers
   */
  List<String> getSenderIdList();

  /**
   * @return list of receiver identifiers
   */
  List<String> getReceiverIdList();

}
