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

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Description about a shuffle group which has not only shuffle descriptions,
 * but the sender and receiver task ids of the corresponding shuffle descriptions.
 */
@DefaultImplementation(ShuffleGroupDescriptionImpl.class)
public interface ShuffleGroupDescription {

  /**
   * @return the name of shuffle group
   */
  String getShuffleGroupName();

  /**
   * Return the name of shuffle names in shuffle group
   *
   * @return list of shuffle names
   */
  List<String> getShuffleNameList();

  ShuffleDescription getShuffleDescription(String shuffleName);

  /**
   * Return list of sender task ids of shuffle named shuffleName in shuffle group
   *
   * @param shuffleName the name of shuffle
   * @return list of sender task ids
   */
  List<String> getSenderIdList(String shuffleName);

  /**
   * Return list of receiver task ids of shuffle named shuffleName in shuffle group
   *
   * @param shuffleName the name of shuffle
   * @return list of sender task ids
   */
  List<String> getReceiverIdList(String shuffleName);

}
