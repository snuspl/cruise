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
package edu.snu.cay.dolphin.examples.matmul;

public final class IndexedElement {

  private final int row;
  private final int column;
  private final double value;

  public IndexedElement(final int row, final int column, final double value) {
    this.row = row;
    this.column = column;
    this.value = value;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  public double getValue() {
    return value;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("IndexedElement[").append(row).append(" , ").append(column).append(" | ")
        .append(value).append("]").toString();
  }
}
