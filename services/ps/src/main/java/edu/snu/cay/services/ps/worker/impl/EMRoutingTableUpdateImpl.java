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
package edu.snu.cay.services.ps.worker.impl;

import edu.snu.cay.services.em.driver.api.EMRoutingTableUpdate;

/**
 * A PS-side implementation of {@link EMRoutingTableUpdate}.
 */
final class EMRoutingTableUpdateImpl implements EMRoutingTableUpdate {

  private final int oldOwnerId;
  private final int newOwnerId;
  private final String newEvalId;
  private final int blockId;

  EMRoutingTableUpdateImpl(final int oldOwnerId, final int newOwnerId, final String newEvalId,
                           final int blockId) {
    this.oldOwnerId = oldOwnerId;
    this.newOwnerId = newOwnerId;
    this.newEvalId = newEvalId;
    this.blockId = blockId;
  }

  @Override
  public int getOldOwnerId() {
    return oldOwnerId;
  }

  @Override
  public int getNewOwnerId() {
    return newOwnerId;
  }

  @Override
  public String getNewEvalId() {
    return newEvalId;
  }

  @Override
  public int getBlockId() {
    return blockId;
  }
}
