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
package edu.snu.cay.services.em.utils;

import edu.snu.cay.services.em.avro.AvroLongRange;
import org.apache.commons.lang.math.LongRange;

/**
 * Utilities for AVRO used in EM.
 */
public final class AvroUtils {
  private AvroUtils() {
  }

  /**
   * Convert LongRange to AvroLongRange.
   */
  public static AvroLongRange toAvroLongRange(final LongRange longRange) {
    return AvroLongRange.newBuilder()
        .setMin(longRange.getMinimumLong())
        .setMax(longRange.getMaximumLong())
        .build();
  }

  /**
   * Convert AvroLongRange to LongRange.
   */
  public static LongRange fromAvroLongRange(final AvroLongRange avroLongRange) {
    return new LongRange(avroLongRange.getMin(), avroLongRange.getMax());
  }
}
