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
package edu.snu.cay.dolphin.bsp.examples.matmul;

import edu.snu.cay.dolphin.bsp.core.ParseException;
import edu.snu.cay.dolphin.bsp.core.UserComputeTask;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataPreRunShuffleOperator;
import edu.snu.cay.dolphin.bsp.core.DataParser;
import edu.snu.cay.dolphin.bsp.groupcomm.interfaces.DataReduceSender;
import org.apache.commons.io.FileUtils;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.*;

public final class MatrixMulCmpTask extends UserComputeTask
    implements DataReduceSender<List<IndexedVector>>, DataPreRunShuffleOperator<Integer, IndexedElement> {

  /**
   * Provides indexed row vectors of the large matrix.
   */
  private final DataParser<List<IndexedVector>> dataParser;

  /**
   * Stores indexed elements of the large matrix grouped by column index.
   */
  private final Map<Integer, List<IndexedElement>> elementsOfLargeMatrix;

  /**
   * The small matrix which consists of ordered row vectors.
   */
  private final Matrix smallMatrix;

  private List<IndexedVector> columnsOfResultMatrix;

  @Inject
  private MatrixMulCmpTask(final DataParser<List<IndexedVector>> dataParser,
                           @Parameter(SmallMatrixFilePath.class) final String filePath) {
    this.dataParser = dataParser;
    this.elementsOfLargeMatrix = new HashMap<>();

    final String rawData = readSmallMatrixFile(filePath);
    this.smallMatrix = parseRawSmallMatrix(rawData);
  }

  private String readSmallMatrixFile(final String filePath) {
    try {
      return FileUtils.readFileToString(new File(filePath));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Matrix parseRawSmallMatrix(final String rawSmallMatrix) {

    final List<double[]> rows = new LinkedList<>();
    for (final String row : rawSmallMatrix.split(System.getProperty("line.separator"))) {
      if (row.startsWith("#") || row.length() == 0) {
        continue;
      }

      final String[] split = row.split("\\s+");
      final int numColumns = split.length - 1;
      final double[] rowVector = new double[numColumns];

      try {
        for (int i = 0; i < numColumns; i++) {
          rowVector[i] = Double.valueOf(split[i + 1]);
        }
      } catch (final NumberFormatException e) {
        throw new RuntimeException(e);
      }
      rows.add(rowVector);
    }

    final double[][] rawData = new double[rows.size()][];
    int index = 0;
    for (final double[] row : rows) {
      rawData[index++] = row;
    }

    return new DenseMatrix(rawData);
  }

  @Override
  public List<Tuple<Integer, IndexedElement>> sendPreRunShuffleData(final int iteration) {
    final List<Tuple<Integer, IndexedElement>> tupleList = new LinkedList<>();
    try {
      for (final IndexedVector rowOfLargeMatrix : dataParser.get()) {
        final int rowIndex = rowOfLargeMatrix.getIndex();
        final Vector rowVector = rowOfLargeMatrix.getVector();

        for (int col = 0; col < rowVector.size(); col++) {
          final IndexedElement indexedElement = new IndexedElement(rowIndex, col, rowVector.get(col));
          tupleList.add(new Tuple<>(col, indexedElement));
        }
      }
    } catch (final ParseException e) {
      throw new RuntimeException(e);
    }

    return tupleList;
  }

  @Override
  public void receivePreRunShuffleData(final int iteration, final List<Tuple<Integer, IndexedElement>> tuples) {
    for (final Tuple<Integer, IndexedElement> tuple : tuples) {
      processIndexedElement(tuple.getKey(), tuple.getValue());
    }
  }

  @Override
  public void run(final int iteration) {
    columnsOfResultMatrix = new ArrayList<>(elementsOfLargeMatrix.size());

    // Send columns of the result matrix
    for (final Integer column : elementsOfLargeMatrix.keySet()) {
      // Create columns of large matrix using indexed elements
      final Vector columnVectorOfLargeMatrix = getColumnVector(elementsOfLargeMatrix.get(column));

      // Calculated columns of the result matrix
      final Vector columnVectorOfResultMatrix = getColumnOfResultMatrix(columnVectorOfLargeMatrix);
      columnsOfResultMatrix.add(new IndexedVector(column, columnVectorOfResultMatrix));
    }
  }

  private void processIndexedElement(final int column, final IndexedElement indexedElement) {
    if (!elementsOfLargeMatrix.containsKey(column)) {
      elementsOfLargeMatrix.put(column, new ArrayList<IndexedElement>());
    }

    elementsOfLargeMatrix.get(column).add(indexedElement);
  }

  @Override
  public List<IndexedVector> sendReduceData(final int iteration) {
    return columnsOfResultMatrix;
  }

  private Vector getColumnVector(final List<IndexedElement> elementList) {
    final Vector columnVector = new DenseVector(elementList.size());
    for (final IndexedElement element : elementList) {
      columnVector.set(element.getRow(), element.getValue());
    }

    return columnVector;
  }

  private Vector getColumnOfResultMatrix(final Vector columnVector) {
    final int size = columnVector.size();
    final Vector multipliedVector = new DenseVector(size);
    for (int i = 0; i < size; i++) {
      multipliedVector.set(i, smallMatrix.viewRow(i).dot(columnVector));
    }

    return multipliedVector;
  }
}
