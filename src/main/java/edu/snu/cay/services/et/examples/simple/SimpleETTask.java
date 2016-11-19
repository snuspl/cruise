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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import org.apache.reef.driver.parameters.DriverIdentifier;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Task code for simple example.
 */
final class SimpleETTask implements Task {
  private static final Logger LOG = Logger.getLogger(SimpleETTask.class.getName());

  private final String elasticTableId;
  private final Identifier driverId;
  private final NetworkConnection networkConnection;

  @Inject
  private SimpleETTask(@Parameter(ETIdentifier.class) final String elasticTableId,
                       @Parameter(DriverIdentifier.class) final String driverIdStr,
                       final IdentifierFactory idFactory,
                       final NetworkConnection networkConnection) {
    this.elasticTableId = elasticTableId;
    this.driverId = idFactory.getNewInstance(driverIdStr);
    this.networkConnection = networkConnection;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}!", elasticTableId);
    networkConnection.send(driverId, "Hello driver!");
    return null;
  }
}
