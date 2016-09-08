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

package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.BlockUpdateNotifyParameter;

import java.util.Set;

/**
 * An implementation of the interface BlockUpdateNotifyParameter class.
 */
public class BlockUpdateNotifyParameterImpl<K> implements BlockUpdateNotifyParameter<K> {
  private final int notifyType;
  private final int blockId;
  private final Set<K> keySet;

  public BlockUpdateNotifyParameterImpl(final int notifyType, final int blockId, final Set<K> keySet) {
    this.notifyType = notifyType;
    this.blockId = blockId;
    this.keySet = keySet;
  }

  @Override
  public int getNotifyType() {
    return notifyType;
  }

  @Override
  public int getUpdatedBlockId() {
    return blockId;
  }

  @Override
  public Set<K> getKeySet() {
    return keySet;
  }
}
