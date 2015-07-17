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
