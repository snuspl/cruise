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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.configuration.parameters.IsMutableTable;
import edu.snu.cay.services.et.configuration.parameters.IsOrderedTable;
import edu.snu.cay.services.et.configuration.parameters.NumTotalBlocks;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A metadata of a table.
 */
public final class TableMetadata {
  private final int numTotalBlocks;
  private final boolean isMutableTable;
  private final boolean isOrderedTable;

  @Inject
  private TableMetadata(@Parameter(NumTotalBlocks.class) final int numTotalBlocks,
                        @Parameter(IsMutableTable.class) final boolean isMutableTable,
                        @Parameter(IsOrderedTable.class) final boolean isOrderedTable) {
    this.numTotalBlocks = numTotalBlocks;
    this.isMutableTable = isMutableTable;
    this.isOrderedTable = isOrderedTable;
  }

  /**
   * @return the number of total blocks comprising this table
   */
  public int getNumTotalBlocks() {
    return numTotalBlocks;
  }

  /**
   * @return True if this table is mutable
   */
  public boolean isMutableTable() {
    return isMutableTable;
  }

  /**
   * @return True it this table's key-space is ordered, not hashed
   */
  public boolean isOrderedTable() {
    return isOrderedTable;
  }

}
