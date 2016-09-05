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
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.dolphin.async.MemoryStoreInitializer;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.exceptions.IdGenerationException;

import javax.inject.Inject;
import java.util.List;

public final class NMFMemoryStoreInitializer implements MemoryStoreInitializer {
  private final NMFDataParser dataParser;
  private final DataIdFactory<Long> idFactory;
  private final MemoryStore<Long> memoryStore;

  @Inject
  public NMFMemoryStoreInitializer(final NMFDataParser dataParser,
                                   final DataIdFactory<Long> idFactory,
                                   final MemoryStore<Long> memoryStore) {
    this.dataParser = dataParser;
    this.idFactory = idFactory;
    this.memoryStore = memoryStore;
  }

  @Override
  public void initialize() {
    final List<NMFData> dataValues = dataParser.parse();

    final List<Long> dataKeys;
    try {
      dataKeys = idFactory.getIds(dataValues.size());
    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }

    memoryStore.putList(dataKeys, dataValues);
  }
}
