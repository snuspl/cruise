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
package edu.snu.cay.dolphin.examples.ml.data;

import edu.snu.cay.dolphin.examples.ml.parameters.IsDenseVector;
import edu.snu.cay.dolphin.examples.ml.sub.DenseRowCodec;
import edu.snu.cay.dolphin.examples.ml.sub.SparseRowCodec;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A Serializer for Dolphin jobs with a single Row dataType.
 * The Row dataType could be backed by either a Dense or Sparse Vector, specified by {@code IsDenseVector}.
 * For example, use this if the job only uses data from {@link ClassificationDenseDataParser}.
 */
public final class RowSerializer implements Serializer {
  /**
   * Key used in Elastic Memory to put/get the data.
   */
  private final String dataType;

  private final Codec<Row> rowCodec;

  @Inject
  private RowSerializer(@Parameter(RowDataType.class) final String dataType,
                        @Parameter(IsDenseVector.class) final boolean isDenseVector,
                        final DenseRowCodec denseRowCodec,
                        final SparseRowCodec sparseRowCodec) {
    this.dataType = dataType;
    this.rowCodec = isDenseVector ? denseRowCodec : sparseRowCodec;
  }

  @Override
  public Codec getCodec(final String name) {
    if (name.equals(dataType)) {
      return rowCodec;
    } else {
      throw new RuntimeException("Unknown name " + name);
    }
  }
}
