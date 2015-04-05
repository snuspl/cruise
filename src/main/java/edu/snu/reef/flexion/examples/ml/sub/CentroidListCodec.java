/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.ml.sub;

import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Codec for encoding and decoding a Centroid List
 */
public final class CentroidListCodec implements Codec<List<Vector>> {

    @Inject
    public CentroidListCodec() {
    }

    @Override
    public final byte[] encode(final List<Vector> list) {

    /* This codec does not assume consistent centroid vector sizes(dimensions).
     * Therefore to specify the initial data size,
     * a quick iteration over the input list to compute
     * the sums of vector sizes is required.
     */
        final int numClusters = list.size();
        int dimension = 0;
        if (numClusters > 0) {
            dimension = list.get(0).size();
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                Integer.SIZE * 2 // for dimension and the number of clusters
                + Double.SIZE * dimension * numClusters);
        try (final DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeInt(numClusters);
            daos.writeInt(dimension);
            for (final Vector centroid : list) {
                for (int i = 0; i < dimension; i++) {
                    daos.writeDouble(centroid.get(i));
                }
            }

        } catch (final IOException e) {
            throw new RuntimeException(e.getCause());
        }


        return baos.toByteArray();
    }

    @Override
    public final List<Vector> decode(final byte[] data) {
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        final List<Vector> list = new ArrayList<>();
        int numClusters = 0;
        int dimension = 0;

        try (final DataInputStream dais = new DataInputStream(bais)) {
            numClusters = dais.readInt();
            dimension = dais.readInt();

            for (int clusterID = 0; clusterID < numClusters; clusterID++) {
                final Vector vector = new DenseVector(dimension);
                for (int i = 0; i < dimension; i++) {
                    vector.set(i, dais.readDouble());
                }
                list.add(vector);
            }

        } catch (final IOException e) {
            throw new RuntimeException(e.getCause());
        }

        return list;
    }
}
