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
package edu.snu.cay.services.em.optimizer.impl;

import edu.snu.cay.services.em.optimizer.api.DataInfo;

/**
 * A plain-old-data implementation of DataInfo.
 */
public final class DataInfoImpl implements DataInfo {
  private int numUnits;

  /**
   * Creates a DataInfo.
   */
  public DataInfoImpl(final int numUnits) {
    this.numUnits = numUnits;
  }

  /**
   * Creates an empty DataInfo that has no data.
   */
  public DataInfoImpl() {
    this(0);
  }

  @Override
  public int getNumUnits() {
    return numUnits;
  }

  /**
   * Updates the number of units. Note that this method is only for testing.
   */
  @Override
  public void setNumUnits(final int numUnits) {
    this.numUnits = numUnits;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DataInfoImpl{");
    sb.append("numUnits=").append(numUnits);
    sb.append('}');
    return sb.toString();
  }
}
