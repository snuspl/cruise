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
package edu.snu.cay.dolphin.examples.sleep;

import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * Codec class used for {@link SleepREEF}.
 *
 * Serialization cost is simulated by sleeping ({@link Thread#sleep(long)}),
 * and network communication cost is indirectly altered by adjusting
 * the size of serialized objects.
 *
 * In the current code, this class is used for both group communication and EM data migration.
 */
final class SleepCodec implements Codec<Object> {

  private final Object object;
  private final int serializedObjectSize;
  private final long encodeRate;
  private final long decodeRate;

  @Inject
  SleepCodec(@Parameter(SleepParameters.GCSerializedObjectSize.class) final int serializedObjectSize,
             @Parameter(SleepParameters.GCEncodeRate.class) final long encodeRate,
             @Parameter(SleepParameters.GCDecodeRate.class) final long decodeRate) {
    this.object = new Object();
    this.serializedObjectSize = serializedObjectSize;
    this.encodeRate = encodeRate;
    this.decodeRate = decodeRate;
  }

  @Override
  public byte[] encode(final Object o) {
    try {
      Thread.sleep(encodeRate);
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while encode-sleeping", e);
    }
    return new byte[serializedObjectSize];
  }

  @Override
  public Object decode(final byte[] bytes) {
    try {
      Thread.sleep(decodeRate);
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while decode-sleeping", e);
    }
    return object;
  }
}
