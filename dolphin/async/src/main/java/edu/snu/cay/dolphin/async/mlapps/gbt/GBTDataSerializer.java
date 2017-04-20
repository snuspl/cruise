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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

/**
 * Serializer that provides codec for (de-)serializing data used in GBT.
 */
final class GBTDataSerializer implements Serializer {
  private final GBTDataCodec gbtDataCodec;

  @Inject
  private GBTDataSerializer(final GBTDataCodec gbtDataCodec) {
    this.gbtDataCodec = gbtDataCodec;
  }

  @Override
  public Codec getCodec() {
    return gbtDataCodec;
  }


}
