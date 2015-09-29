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
package edu.snu.cay.services.em.evaluator;

import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.reef.io.serialization.Codec;

import java.util.Collection;

/**
 * Implementation of Update to add the data from MemoryStore when apply() is called.
 */
class Add implements Update {
  private final String dataType;
  private final Codec codec;
  private final Collection<UnitIdPair> unitIdPairs;

  Add(final String dataType, final Codec codec, final Collection<UnitIdPair> unitIdPairs) {
    this.dataType = dataType;
    this.codec = codec;
    this.unitIdPairs = unitIdPairs;
  }

  @Override
  public Type getType() {
    return Type.ADD;
  }

  @Override
  public void apply(final MemoryStore memoryStore) {
    for (final UnitIdPair unitIdPair : unitIdPairs) {
      final byte[] data = unitIdPair.getUnit().array();
      final long id = unitIdPair.getId();
      memoryStore.getElasticStore().put(dataType, id, codec.decode(data));
    }
  }
}
