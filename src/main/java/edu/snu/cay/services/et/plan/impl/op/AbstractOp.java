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
package edu.snu.cay.services.et.plan.impl.op;

import edu.snu.cay.services.et.plan.api.Op;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract class of {@link Op}.
 * It's a base class for ET operation implementations.
 */
public abstract class AbstractOp implements Op {
  private static final AtomicLong OP_ID_COUNTER = new AtomicLong(0);
  private final long opId;
  private final OpType opType;

  AbstractOp(final OpType opType) {
    this.opId = OP_ID_COUNTER.getAndIncrement();
    this.opType = opType;
  }

  @Override
  public OpType getOpType() {
    return opType;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractOp)) {
      return false;
    }

    final AbstractOp that = (AbstractOp) o;

    return opId == that.opId;
  }

  @Override
  public int hashCode() {
    return (int) (opId ^ (opId >>> 32));
  }
}
