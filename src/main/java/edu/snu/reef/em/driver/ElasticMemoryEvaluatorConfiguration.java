package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryMessageCodec;
import edu.snu.reef.em.ns.NSWrapperDriver;
import edu.snu.reef.em.task.ElasticMemoryClient;
import edu.snu.reef.em.task.ElasticMemoryStore;
import edu.snu.reef.em.task.MemoryStore;
import edu.snu.reef.em.task.NSWrapperToContext;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

@DriverSide
public class ElasticMemoryEvaluatorConfiguration {

  final NSWrapperDriver nsWrapperDriver;

  @Inject
  private ElasticMemoryEvaluatorConfiguration(final NSWrapperDriver nsWrapperDriver) {
    this.nsWrapperDriver = nsWrapperDriver;
  }

  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, ElasticMemoryClient.class)
        .bindSetEntry(ContextStartHandlers.class, NSWrapperToContext.BindNSWrapperToContext.class)
        .bindSetEntry(ContextStopHandlers.class, NSWrapperToContext.UnbindNSWrapperToContext.class)
        .build();
  }

  public Configuration getServiceConfiguration() {

    final Configuration nsWrapperConf =
        nsWrapperDriver.getConfiguration(ElasticMemoryMessageCodec.class,
                                         ElasticMemoryMessageHandlerWrapperImpl.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStore.class)
        .build();

    final Configuration bindConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, ElasticMemoryStore.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindConf);
  }
}
