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
 * WIdoubleHOUdouble WARRANdoubleIES OR CONDIdoubleIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.mlapps.nmf;

import edu.snu.cay.common.math.linalg.Vector;
import org.apache.reef.io.network.util.Pair;

import java.util.List;

/**
 * Data object for non-negative matrix factorization.
 */
final class NMFData {

  private final int rowIndex;
  private final List<Pair<Integer, Double>> columns;
  private final Vector vector;

  NMFData(final int rowIndex,
          final List<Pair<Integer, Double>> columns,
          final Vector vector) {
    this.rowIndex = rowIndex;
    this.columns = columns;
    this.vector = vector;
  }

  int getRowIndex() {
    return rowIndex;
  }

  List<Pair<Integer, Double>> getColumns() {
    return columns;
  }

  Vector getVector() {
    return vector;
  }
}
