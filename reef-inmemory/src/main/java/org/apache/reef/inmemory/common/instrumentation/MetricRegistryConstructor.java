package org.apache.reef.inmemory.common.instrumentation;

import com.codahale.metrics.MetricRegistry;
import com.microsoft.tang.ExternalConstructor;

import javax.inject.Inject;

/**
 * Construct a MetricRegistry, for aggregating metrics.
 */
public final class MetricRegistryConstructor implements ExternalConstructor<MetricRegistry> {

  @Inject
  public MetricRegistryConstructor() {
  }

  @Override
  public MetricRegistry newInstance() {
    return new MetricRegistry();
  }
}
