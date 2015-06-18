package edu.snu.reef.em.task;

import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

public final class ElasticMemoryService {
  public static Configuration getServiceConfiguration() {
    final Configuration partialServiceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryServiceClient.class)
        .build();

    return Tang.Factory.getTang()
        .newConfigurationBuilder(partialServiceConf)
        .bindImplementation(MemoryStoreClient.class, ElasticMemoryServiceClient.class)
        .build();
  }
}
