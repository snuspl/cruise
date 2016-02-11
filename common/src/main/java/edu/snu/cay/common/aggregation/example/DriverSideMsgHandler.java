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
package edu.snu.cay.common.aggregation.example;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.param.Parameters;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * This receives aggregation messages, since driver is an aggregation master.
 * Provides a way to check whether all messages are arrived or not via {@code validate()} method.
 */
@DriverSide
public final class DriverSideMsgHandler implements EventHandler<AggregationMessage> {
  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  private final CountDownLatch msgCountDown;

  @Inject
  private DriverSideMsgHandler(@Parameter(Parameters.Splits.class) final int split) {
    this.msgCountDown = new CountDownLatch(split);
  }

  @Override
  public void onNext(final AggregationMessage message) {
    LOG.log(Level.INFO, "Received aggregation message {0}", message);
    msgCountDown.countDown();
  }

  /**
   * Checks whether all messages are arrived or not.
   * If not, wait until all messages are arrived or an interruption(e.g., timeout) occurs.
   * @throws RuntimeException if an interruption occurs before finish receiving all messages
   */
  public void validate() {
    LOG.log(Level.INFO, "Validating...");
    try {
      msgCountDown.await();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Validation failed due to {0}", e);
      throw new RuntimeException(e);
    }
  }
}
