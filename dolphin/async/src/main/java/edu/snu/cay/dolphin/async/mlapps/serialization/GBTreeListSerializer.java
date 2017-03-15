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
package edu.snu.cay.dolphin.async.mlapps.serialization;

import edu.snu.cay.dolphin.async.mlapps.gbt.tree.GBTree;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.util.List;

/**
 * Provides the GBTreeListCodec that (de-)serializes.
 */
public final class GBTreeListSerializer implements Serializer {
  private final Codec<List<GBTree>> gbTreeListCodec;

  @Inject
  private GBTreeListSerializer(final GBTreeListCodec gbTreeListCodec) {
    this.gbTreeListCodec = gbTreeListCodec;
  }

  public Codec getCodec() {
    return this.gbTreeListCodec;
  }
}
