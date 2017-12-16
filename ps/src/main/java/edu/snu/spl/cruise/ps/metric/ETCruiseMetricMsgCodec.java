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
package edu.snu.spl.cruise.ps.metric;

import edu.snu.spl.cruise.ps.metric.avro.CruiseWorkerMetrics;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

/**
 * Codec that (de-)serializes the Cruise-specific metrics.
 */
public final class ETCruiseMetricMsgCodec implements Codec<CruiseWorkerMetrics> {
  @Inject
  private ETCruiseMetricMsgCodec() {
  }

  @Override
  public CruiseWorkerMetrics decode(final byte[] bytes) {
    return AvroUtils.fromBytes(bytes, CruiseWorkerMetrics.class);
  }

  @Override
  public byte[] encode(final CruiseWorkerMetrics cruiseMetrics) {
    return AvroUtils.toBytes(cruiseMetrics, CruiseWorkerMetrics.class);
  }
}
