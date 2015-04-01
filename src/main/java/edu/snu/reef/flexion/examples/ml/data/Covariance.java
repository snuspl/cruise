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
package edu.snu.reef.flexion.examples.ml.data;

import org.apache.mahout.math.Matrix;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;

/**
 * A cluster covariance implementation of EM clustering
 */
public final class Covariance implements Serializable {

    private final int clusterId;
    private Matrix matrix;

    /**
     * This constructor does not create a deep copy of @param matrix.
     */
    public Covariance(final int clusterId, final Matrix matrix) {
        this.clusterId = clusterId;
        this.matrix = matrix;
    }

    /**
     * A copy constructor that creates a deep copy of a covariance matrix.
     *
     * The newly created covariance matrix does not reference
     * anything from the original covariance matrix.
     */
    public Covariance(final Covariance covariance) {
        this.clusterId = covariance.clusterId;
        this.matrix = covariance.matrix.clone();
    }

    public final Matrix getMatrix() { return matrix; }

    public final int getClusterId() {
        return clusterId;
    }

    @SuppressWarnings("boxing")
    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder("EMCovariance(");
        try (final Formatter formatter = new Formatter(b, Locale.US)) {
            formatter.format("Id %d,\n", this.clusterId);
            for (int i = 0; i < this.matrix.rowSize(); ++i) {
                for (int j = 0; j < this.matrix.columnSize() - 1; ++j) {
                    formatter.format("%1.3f, ", this.matrix.get(i, j));
                }
                formatter.format("%1.3f\n", this.matrix.get(i, this.matrix.columnSize() - 1));
            }
        }
        b.append(')');
        return b.toString();
    }

}
