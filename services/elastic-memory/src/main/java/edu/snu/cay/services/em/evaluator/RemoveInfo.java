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

import java.util.Collection;

/**
 * Holds the information of data to remove from MemoryStore after migration completes.
 */
class RemoveInfo implements UpdateInfo {
  private String dataType;
  private Collection<Long> ids;

  RemoveInfo(final String dataType, final Collection<Long> ids) {
    this.dataType = dataType;
    this.ids = ids;
  }

  @Override
  public Type getType() {
    return Type.REMOVE;
  }

  String getDataType() {
    return dataType;
  }

  Collection<Long> getIds() {
    return ids;
  }
}
