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
package edu.snu.spl.cruise.services.et.exceptions;

import org.apache.reef.annotations.audience.EvaluatorSide;

/**
 * Indicates that the specified block does not exist in
 * {@link edu.snu.spl.cruise.services.et.evaluator.impl.BlockStore}.
 */
@EvaluatorSide
public final class BlockNotExistsException extends Exception {
  private final int blockId;
  public BlockNotExistsException(final int blockId) {
    super("Block " + blockId + " does not exist");
    this.blockId = blockId;
  }
  public int getBlockIdx() {
    return blockId;
  }
}
