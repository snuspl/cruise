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
package edu.snu.cay.dolphin.bsp.mlapps.data;

import edu.snu.cay.dolphin.bsp.mlapps.sub.DenseRowCodec;
import edu.snu.cay.dolphin.bsp.mlapps.parameters.IsDenseVector;
import edu.snu.cay.dolphin.bsp.mlapps.sub.SparseRowCodec;
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
  private final Codec<Row> rowCodec;

  @Inject
  private RowSerializer(@Parameter(IsDenseVector.class) final boolean isDenseVector,
                        final DenseRowCodec denseRowCodec,
                        final SparseRowCodec sparseRowCodec) {
    this.rowCodec = isDenseVector ? denseRowCodec : sparseRowCodec;
  }

  @Override
  public Codec getCodec() {
    return rowCodec;
  }
}
