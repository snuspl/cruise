/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.spl.cruise.services.et.common.api;

import edu.snu.spl.cruise.services.et.common.impl.NetworkConnectionImpl;
import edu.snu.spl.cruise.services.et.exceptions.AlreadyConnectedException;
import edu.snu.spl.cruise.services.et.exceptions.NotConnectedException;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Network connection for evaluators and the driver.
 * @param <T> message type
 */
@DefaultImplementation(NetworkConnectionImpl.class)
public interface NetworkConnection<T> {
  /**
   * Establish a connection to ET network layer.
   * @throws AlreadyConnectedException when a network connection already exists
   */
  void setup(String localEndpointId) throws AlreadyConnectedException;

  /**
   * TODO #8: Implement and use msg protocol for Elastic-Tables
   * Send msg through connection.
   * @param destId the identifier of the destination
   * @param msg the payload to send
   * @throws NotConnectedException when network connection is not established
   * @throws NetworkException when fail to send a msg
   */
  void send(String destId, T msg) throws NotConnectedException, NetworkException;
}
