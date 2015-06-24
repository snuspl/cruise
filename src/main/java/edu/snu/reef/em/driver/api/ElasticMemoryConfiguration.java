package edu.snu.reef.em.driver.api;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.Configuration;

/**
 * Configuration class for setting evaluator configurations of ElasticMemoryService
 */
@DriverSide
public interface ElasticMemoryConfiguration {

  /**
   * @return configuration that should be merged with a ContextConfiguration to form a context
   */
  Configuration getContextConfiguration();

  /**
   * @return service configuration that should be passed along with a ContextConfiguration
   */
  Configuration getServiceConfiguration();
}
