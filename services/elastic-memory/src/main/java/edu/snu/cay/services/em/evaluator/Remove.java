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

import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import org.apache.commons.lang.math.LongRange;

import java.util.Collection;

/**
 * Implementation of Update to remove the data from MemoryStore when apply() is called.
 */
final class Remove implements Update {
  private String dataType;
  private Collection<LongRange> ranges;

  Remove(final String dataType, final Collection<LongRange> ranges) {
    this.dataType = dataType;
    this.ranges = ranges;
  }

  @Override
  public Type getType() {
    return Type.REMOVE;
  }

  @Override
  public Collection<LongRange> getRanges() {
    return ranges;
  }

  @Override
  public void apply(final MemoryStore memoryStore) {
    for (final LongRange range : ranges) {
      memoryStore.removeRange(dataType, range.getMinimumLong(), range.getMaximumLong());
    }
  }
}
