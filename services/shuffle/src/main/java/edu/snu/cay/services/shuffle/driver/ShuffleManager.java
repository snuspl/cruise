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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.network.ShuffleControlMessage;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.transport.LinkListener;

/**
 * Driver-side interface which communicates with corresponding Shuffle instances in evaluators.
 */
@DriverSide
public interface ShuffleManager {

  /**
   * Return a configuration of the shuffle description for endPointId.
   * It returns Optional.empty if the endPointId is not included in any shuffles.
   *
   * @param endPointId end point id
   * @return optional shuffle configuration
   */
  Optional<Configuration> getShuffleConfiguration(String endPointId);

  /**
   * @return a shuffle description handled by the ShuffleController
   */
  ShuffleDescription getShuffleDescription();

  /**
   * @return an event handler for shuffle control messages.
   */
  EventHandler<Message<ShuffleControlMessage>> getControlMessageHandler();

  /**
   * @return a link listener for shuffle control messages.
   */
  LinkListener<Message<ShuffleControlMessage>> getControlLinkListener();
}
