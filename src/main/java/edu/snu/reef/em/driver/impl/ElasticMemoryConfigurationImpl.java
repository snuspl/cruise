package edu.snu.reef.em.driver.impl;

import edu.snu.reef.em.driver.api.ElasticMemoryConfiguration;
import edu.snu.reef.em.msg.ElasticMemoryMessageCodec;
import edu.snu.reef.em.ns.NSWrapperDriver;
import edu.snu.reef.em.task.*;
import edu.snu.reef.em.utils.ElasticMemoryMessageBroadcastHandler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

@DriverSide
public final class ElasticMemoryConfigurationImpl implements ElasticMemoryConfiguration {

  private final NSWrapperDriver nsWrapperDriver;

  @Inject
  private ElasticMemoryConfigurationImpl(final NSWrapperDriver nsWrapperDriver) {
    this.nsWrapperDriver = nsWrapperDriver;
  }

  @Override
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NSHandlerBind.ContextStartHandler.class)
        .bindSetEntry(ContextStartHandlers.class, NSWrapperToContext.BindNSWrapperToContext.class)
        .bindSetEntry(ContextStopHandlers.class, NSWrapperToContext.UnbindNSWrapperToContext.class)
        .bindSetEntry(ContextStopHandlers.class, NSHandlerBind.ContextStopHandler.class)
        .build();
  }

  @Override
  public Configuration getServiceConfiguration() {
    final Configuration nsWrapperConf =
        nsWrapperDriver.getConfiguration(ElasticMemoryMessageCodec.class,
                                         ElasticMemoryMessageBroadcastHandler.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStore.class)
        .build();

    final Configuration bindImplConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, ElasticMemoryStore.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindImplConf);
  }
}
