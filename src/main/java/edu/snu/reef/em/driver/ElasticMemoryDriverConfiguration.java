package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgHandler;
import edu.snu.reef.em.msg.ElasticMemoryMessageCodec;
import edu.snu.reef.em.ns.*;
import edu.snu.reef.em.task.ElasticMemoryClient;
import edu.snu.reef.em.task.ElasticMemoryStoreClient;
import edu.snu.reef.em.task.MemoryStoreClient;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextMessageHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

public class ElasticMemoryDriverConfiguration {

  final NSWrapperDriver nsWrapperDriver;

  @Inject
  private ElasticMemoryDriverConfiguration(final NSWrapperDriver nsWrapperDriver) {
    this.nsWrapperDriver = nsWrapperDriver;
  }

  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageHandlers.class, ElasticMemoryCtrlMsgHandler.class)
        .build();
  }

  public Configuration getServiceConfiguration() {
    final Configuration nsWrapperConf =
        nsWrapperDriver.getConfiguration(ElasticMemoryMessageCodec.class,
                                         ElasticMemoryMessageHandlerWrapperImpl.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStoreClient.class)
        .set(ServiceConfiguration.ON_CONTEXT_STARTED, ElasticMemoryClient.class)
        .build();

    final Configuration bindConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStoreClient.class, ElasticMemoryStoreClient.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindConf);
  }
}
