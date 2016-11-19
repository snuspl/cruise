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
package edu.snu.cay.services.et.exceptions;

import org.apache.reef.wake.Identifier;

/**
 * Indicates that network connection is already established.
 */
public final class AlreadyConnectedException extends RuntimeException {
  private final Identifier connectionFactoryId;
  private final Identifier localEndPointId;

  public AlreadyConnectedException(final Identifier connectionFactoryId, final Identifier localEndPointId) {
    super(String.format("NetworkConnection for %s/%s already exists.",
        connectionFactoryId.toString(), localEndPointId.toString()));
    this.connectionFactoryId = connectionFactoryId;
    this.localEndPointId = localEndPointId;
  }

  public Identifier getConnectionFactoryId() {
    return connectionFactoryId;
  }

  public Identifier getLocalEndPointId() {
    return localEndPointId;
  }
}
