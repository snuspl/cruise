/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.metric;

import edu.snu.cay.services.et.avro.MetricMsg;
import edu.snu.cay.services.et.driver.api.MetricReceiver;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of Metric receiver for Dolphin on ET.
 */
public final class ETDolphinMetricReceiver implements MetricReceiver {
  private static final Logger LOG = Logger.getLogger(ETDolphinMetricReceiver.class.getName());

  @Inject
  ETDolphinMetricReceiver() {
  }

  @Override
  public void onNext(final MetricMsg metricMsg) {
    // TODO #1071: How do we?
    LOG.log(Level.INFO, "Received metric: {0}", metricMsg);
  }
}
