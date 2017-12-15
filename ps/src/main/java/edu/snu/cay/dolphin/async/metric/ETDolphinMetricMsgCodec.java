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

import edu.snu.cay.dolphin.async.metric.avro.DolphinWorkerMetrics;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

/**
 * Codec that (de-)serializes the Dolphin-specific metrics.
 */
public final class ETDolphinMetricMsgCodec implements Codec<DolphinWorkerMetrics> {
  @Inject
  private ETDolphinMetricMsgCodec() {
  }

  @Override
  public DolphinWorkerMetrics decode(final byte[] bytes) {
    return AvroUtils.fromBytes(bytes, DolphinWorkerMetrics.class);
  }

  @Override
  public byte[] encode(final DolphinWorkerMetrics dolphinMetrics) {
    return AvroUtils.toBytes(dolphinMetrics, DolphinWorkerMetrics.class);
  }
}
