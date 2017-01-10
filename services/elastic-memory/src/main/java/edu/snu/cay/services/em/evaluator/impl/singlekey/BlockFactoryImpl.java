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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.evaluator.api.Block;
import edu.snu.cay.services.em.evaluator.api.BlockFactory;
import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;

import javax.inject.Inject;

/**
 * An implementation of {@link BlockFactory} that produces a block for single-key operation.
 */
public final class BlockFactoryImpl implements BlockFactory {

  /**
   * An update function to be used in {@link BlockImpl#update}.
   * We assume that there's only one function for the store.
   */
  private final EMUpdateFunction emUpdateFunction;

  @Inject
  private BlockFactoryImpl(final EMUpdateFunction emUpdateFunction) {
    this.emUpdateFunction = emUpdateFunction;
  }

  @Override
  public Block newBlock() {
    return new BlockImpl(emUpdateFunction);
  }
}
