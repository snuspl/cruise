package edu.snu.reef.flexion.core;


import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

import java.util.logging.Logger;

/**
 * Key-value store service used to pass the current stage's result to the next stage
 * Should be inserted alongside a context.
 */
public class KeyValueStoreService {

  private static Logger LOG = Logger.getLogger(KeyValueStoreService.class.getName());

  public static Configuration getServiceConfiguration() {
    Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, KeyValueStore.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .build();
  }

}
