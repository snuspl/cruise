package edu.snu.reef.dolphin.core.metric;

import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextMessageSources;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;

/**
 * A simple metric tracker class given in the form of a service.
 * Should be inserted alongside a context.
 */
@Unit
public final class MetricTrackerService {

  /**
   * Return the service configuration for the metric tracker service
   * @return  service configuration for the metric tracker service
   */
  public static Configuration getServiceConfiguration() {
    final Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, MetricManager.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .build();
  }

  /**
   * Return the context configuration for the metric tracker service
   * @return  context configuration for the metric tracker service
   */
  public static Configuration getContextConfiguration() {
    final Configuration contextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, MetricTrackerService.class.getName())
        .set(ContextConfiguration.ON_SEND_MESSAGE, MetricManager.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(contextConf)
        .build();
  }

  /**
   * Add a context message source to the pre-existed context configuration
   * @param previousConfiguration pre-existed context configuration
   * @return
   */
  public static Configuration getContextConfiguration(final Configuration previousConfiguration) {
    Configuration contextConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageSources.class, MetricManager.class)
        .build();

    return Configurations.merge(contextConf, previousConfiguration);
  }
}
