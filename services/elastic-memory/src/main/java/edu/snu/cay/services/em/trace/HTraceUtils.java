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
package edu.snu.cay.services.em.trace;

import org.apache.htrace.TraceInfo;

/**
 * Utility methods to convert between HTrace and Avro.
 */
public final class HTraceUtils {
  private HTraceUtils() {
  }

  /**
   * Convert from Avro to HTrace.
   * @param avroTraceInfo Avro trace info
   * @return
   */
  public static TraceInfo fromAvro(final AvroTraceInfo avroTraceInfo) {
    if (avroTraceInfo == null) {
      return null;
    } else {
      return new TraceInfo(avroTraceInfo.getTraceId(), avroTraceInfo.getSpanId());
    }
  }

  /**
   * Convert from HTrace to Avro.
   * @param traceInfo HTrace trace info
   * @return
   */
  public static AvroTraceInfo toAvro(final TraceInfo traceInfo) {
    if (traceInfo == null) {
      return null;
    } else {
      return AvroTraceInfo.newBuilder()
          .setTraceId(traceInfo.traceId)
          .setSpanId(traceInfo.spanId)
          .build();
    }
  }
}
