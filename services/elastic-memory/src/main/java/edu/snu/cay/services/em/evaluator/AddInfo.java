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

import java.util.Collection;

/**
 * Holds the information of data to add to MemoryStore after migration completes.
 */
class AddInfo implements UpdateInfo {
  private final String dataType;
  private final Collection<UnitIdPair> unitIdPairs;

  AddInfo(final String dataType, final Collection<UnitIdPair> unitIdPairs) {
    this.dataType = dataType;
    this.unitIdPairs = unitIdPairs;
  }

  @Override
  public Type getType() {
    return Type.ADD;
  }

  String getDataType() {
    return dataType;
  }

  Collection<UnitIdPair> getUnitIdPairs() {
    return unitIdPairs;
  }
}
