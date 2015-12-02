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
  private final long encodeTime;
  private final long decodeTime;

  @Inject
  SleepCodec(@Parameter(SleepParameters.GCEncodeTime.class) final long encodeTime,
             @Parameter(SleepParameters.GCDecodeTime.class) final long decodeTime) {
    this.object = new Object();
    this.encodeTime = encodeTime;
    this.decodeTime = decodeTime;
  }

  @Override
  public byte[] encode(final Object o) {
    try {
      Thread.sleep(encodeTime);
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while encode-sleeping", e);
    }
    return new byte[1];
  }

  @Override
  public Object decode(final byte[] bytes) {
    try {
      Thread.sleep(decodeTime);
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException while decode-sleeping", e);
    }
    return object;
  }
}
