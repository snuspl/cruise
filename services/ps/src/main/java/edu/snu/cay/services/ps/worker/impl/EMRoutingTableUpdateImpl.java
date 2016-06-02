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

import java.util.List;

/**
 * A PS-side implementation of {@link EMRoutingTableUpdate}.
 */
final class EMRoutingTableUpdateImpl implements EMRoutingTableUpdate {

  private final int oldOwnerId;
  private final int newOwnerId;
  private final String newEvalId;
  private final List<Integer> blockIds;

  EMRoutingTableUpdateImpl(final int oldOwnerId, final int newOwnerId, final String newEvalId,
                           final List<Integer> blockIds) {
    this.oldOwnerId = oldOwnerId;
    this.newOwnerId = newOwnerId;
    this.newEvalId = newEvalId;
    this.blockIds = blockIds;
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
  public List<Integer> getBlockIds() {
    return blockIds;
  }
}
