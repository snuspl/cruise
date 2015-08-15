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
package edu.snu.cay.dolphin.examples.ml.sub;

import edu.snu.cay.dolphin.examples.ml.parameters.IsCovarianceDiagonal;
import edu.snu.cay.dolphin.examples.ml.data.ClusterSummary;
import org.apache.mahout.math.*;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Codec for encoding and decoding a Cluster Summary List.
 */
public final class ClusterSummaryListCodec implements Codec<List<ClusterSummary>> {
  private final boolean isDiagonalCovariance;

  @Inject
  public ClusterSummaryListCodec(@Parameter(IsCovarianceDiagonal.class) final boolean isDiagonalCovariance) {
    this.isDiagonalCovariance = isDiagonalCovariance;
  }

  @Override
  public byte[] encode(final List<ClusterSummary> list) {

    final int numClusters = list.size();
    int dimension = 0;
    if (numClusters > 0) {
      dimension = list.get(0).getCentroid().size();
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(
        Integer.SIZE * 2 // for dimension and the number of clusters
            + Double.SIZE * numClusters // for prior
            + Double.SIZE * dimension * numClusters// for centroids
            + Double.SIZE * (isDiagonalCovariance ?
            dimension : dimension * dimension) * numClusters); // for covariance matrices

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(numClusters);
      daos.writeInt(dimension);

      for (final ClusterSummary clusterSummary : list) {
        daos.writeDouble(clusterSummary.getPrior());
        for (int i = 0; i < dimension; i++) {
          daos.writeDouble(clusterSummary.getCentroid().get(i));
        }
        if (isDiagonalCovariance) {
          for (int i = 0; i < dimension; i++) {
            daos.writeDouble(clusterSummary.getCovariance().get(i, i));
          }
        } else {
          for (int i = 0; i < dimension; i++) {
            for (int j = 0; j < dimension; j++) {
              daos.writeDouble(clusterSummary.getCovariance().get(i, j));
            }
          }
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return baos.toByteArray();
  }

  @Override
  public List<ClusterSummary> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final List<ClusterSummary> resultList = new ArrayList<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int numClusters = dais.readInt();
      final int dimension = dais.readInt();

      for (int i = 0; i < numClusters; i++) {
        final double prior = dais.readDouble();
        final Vector vector = new DenseVector(dimension);
        for (int j = 0; j < dimension; j++) {
          vector.set(j, dais.readDouble());
        }
        Matrix matrix = null;
        if (isDiagonalCovariance) {
          matrix = new SparseMatrix(dimension, dimension);
          for (int j = 0; j < dimension; j++) {
            matrix.set(j, j, dais.readDouble());
          }
        } else {
          matrix = new DenseMatrix(dimension, dimension);
          for (int j = 0; j < dimension; j++) {
            for (int k = 0; k < dimension; k++) {
              matrix.set(j, k, dais.readDouble());
            }
          }
        }
        resultList.add(new ClusterSummary(prior, vector, matrix));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return resultList;
  }
}
