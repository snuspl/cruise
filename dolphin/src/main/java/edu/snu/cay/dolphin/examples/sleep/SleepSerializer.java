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

import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * The EM {@link Serializer} for SleepREEF.
 * No meaningful data is passed between evaluators.
 */
public final class SleepSerializer implements Serializer {

  private final SleepCodec sleepCodec;

  @Inject
  private SleepSerializer(@Parameter(SleepParameters.EMSerializedObjectSize.class) final int serializedObjectSize,
                          @Parameter(SleepParameters.EMEncodeRate.class) final long encodeRate,
                          @Parameter(SleepParameters.EMDecodeRate.class) final long decodeRate) {
    this.sleepCodec = new SleepCodec(serializedObjectSize, encodeRate, decodeRate);
  }

  @Override
  public Codec getCodec(final String name) {
    return sleepCodec;
  }
}
