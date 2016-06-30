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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

/**
 * Provides the DenseVectorCodec that (de-)serializes
 * DenseVector backed by breeze.
 */
// TODO #402: Remove this duplicate of {@code DenseVectorCodec} in dolphin submodule.
public final class DenseVectorSerializer implements Serializer {
  private final DenseVectorCodec denseVectorCodec;

  @Inject
  private DenseVectorSerializer(final DenseVectorCodec denseVectorCodec) {
    this.denseVectorCodec = denseVectorCodec;
  }

  @Override
  public Codec getCodec() {
    return denseVectorCodec;
  }
}
