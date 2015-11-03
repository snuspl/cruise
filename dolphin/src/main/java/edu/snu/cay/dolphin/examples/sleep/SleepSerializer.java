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

import javax.inject.Inject;

/**
 * The EM {@link Serializer} for SleepREEF.
 * No meaningful data is passed between evaluators, and thus
 * this serializer provides a codec that creates minimal information.
 */
public final class SleepSerializer implements Serializer {

  private final EmptyCodec emptyCodec;

  @Inject
  private SleepSerializer() {
    this.emptyCodec = new EmptyCodec();
  }

  @Override
  public Codec getCodec(final String name) {
    return emptyCodec;
  }

  private final class EmptyCodec implements Codec<Object> {

    private final Object object;

    private EmptyCodec() {
      this.object = new Object();
    }

    @Override
    public byte[] encode(final Object o) {
      return new byte[0];
    }

    @Override
    public Object decode(final byte[] bytes) {
      return object;
    }
  }
}
