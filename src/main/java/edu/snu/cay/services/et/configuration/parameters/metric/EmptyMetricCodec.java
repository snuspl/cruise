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
package edu.snu.cay.services.et.configuration.parameters.metric;

import org.apache.reef.io.network.impl.StreamingCodec;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * A codec that can be used when there is no custom metrics.
 */
public final class EmptyMetricCodec implements StreamingCodec<Void> {
  @Inject
  private EmptyMetricCodec() {
  }

  @Override
  public void encodeToStream(final Void aVoid, final DataOutputStream dataOutputStream) {
    // do nothing
  }

  @Override
  public Void decodeFromStream(final DataInputStream dataInputStream) {
    // do nothing
    return null;
  }

  @Override
  public Void decode(final byte[] bytes) {
    // do nothing
    return null;
  }

  @Override
  public byte[] encode(final Void aVoid) {
    // do nothing
    return new byte[0];
  }
}
