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
package edu.snu.cay.dolphin.bsp.examples.ml.sub;

import edu.snu.cay.dolphin.bsp.examples.ml.parameters.IsCovarianceDiagonal;
import edu.snu.cay.dolphin.bsp.examples.ml.data.ClusterStats;
import org.apache.mahout.math.*;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * Codec for encoding and decoding a map of Integer vs. clusterStats.
 */
public final class MapOfIntClusterStatsCodec implements Codec<Map<Integer, ClusterStats>> {
  private final boolean diagonalCovariance;

  @Inject
  public MapOfIntClusterStatsCodec(@Parameter(IsCovarianceDiagonal.class) final boolean diagonalCovariance) {
    this.diagonalCovariance = diagonalCovariance;
  }

  @Override
  public byte[] encode(final Map<Integer, ClusterStats> map) {
    final int mapSize = map.size();
    int dimension = 0;
    if (mapSize > 0) {
      for (final ClusterStats entry : map.values()) {
        dimension = entry.getPointSum().size();
        break;
      }
    }

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE * 2 // for dimension & map size
        + Integer.SIZE * mapSize// for cluster id
        + Double.SIZE * mapSize // for probability sum
        + Double.SIZE * dimension * mapSize// for point sum
        + Double.SIZE * (diagonalCovariance ? dimension : dimension * dimension) * mapSize); // for outer product sum

    try (final DataOutputStream daos = new DataOutputStream(baos)) {
      daos.writeInt(map.size());
      daos.writeInt(dimension);
      for (final Integer id : map.keySet()) {
        daos.writeInt(id);
        final ClusterStats clusterSummary = map.get(id);
        daos.writeDouble(clusterSummary.getProbSum());
        for (int j = 0; j < clusterSummary.getPointSum().size(); j++) {
          daos.writeDouble(clusterSummary.getPointSum().get(j));
        }
        if (diagonalCovariance) {
          for (int i = 0; i < dimension; i++) {
            daos.writeDouble(clusterSummary.getOutProdSum().get(i, i));
          }
        } else {
          for (int i = 0; i < dimension; i++) {
            for (int j = 0; j < dimension; j++) {
              daos.writeDouble(clusterSummary.getOutProdSum().get(i, j));
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
  public Map<Integer, ClusterStats> decode(final byte[] data) {
    final ByteArrayInputStream bais = new ByteArrayInputStream(data);
    final Map<Integer, ClusterStats> resultMap = new HashMap<>();

    try (final DataInputStream dais = new DataInputStream(bais)) {
      final int mapSize = dais.readInt();
      final int dimension = dais.readInt();
      for (int i = 0; i < mapSize; i++) {
        final int id = dais.readInt();
        final double probSum = dais.readDouble();
        final Vector pointSum = new DenseVector(dimension);
        for (int j = 0; j < dimension; j++) {
          pointSum.set(j, dais.readDouble());
        }
        Matrix outProdSum = null;
        if (diagonalCovariance) {
          outProdSum = new SparseMatrix(dimension, dimension);
          for (int j = 0; j < dimension; j++) {
            outProdSum.set(j, j, dais.readDouble());
          }
        } else {
          outProdSum = new DenseMatrix(dimension, dimension);
          for (int j = 0; j < dimension; j++) {
            for (int k = 0; k < dimension; k++) {
              outProdSum.set(j, k, dais.readDouble());
            }
          }
        }
        resultMap.put(id, new ClusterStats(outProdSum, pointSum, probSum));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e.getCause());
    }

    return resultMap;
  }
}
