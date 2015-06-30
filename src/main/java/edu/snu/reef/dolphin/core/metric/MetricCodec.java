package edu.snu.reef.dolphin.core.metric;

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.remote.Codec;

import java.util.Map;

/**
 * Interface of codecs for metrics
 */
@DefaultImplementation(DefaultMetricCodecImpl.class)
public interface MetricCodec extends Codec<Map<String, Double>> {
}
