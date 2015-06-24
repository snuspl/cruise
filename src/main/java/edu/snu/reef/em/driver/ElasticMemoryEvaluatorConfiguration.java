package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgHandler;
import edu.snu.reef.em.msg.ElasticMemoryMessageCodec;
import edu.snu.reef.em.ns.*;
import edu.snu.reef.em.task.ElasticMemoryClient;
import edu.snu.reef.em.task.ElasticMemoryStoreClient;
import edu.snu.reef.em.task.MemoryStoreClient;
import edu.snu.reef.em.task.NSWrapperToContext;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextMessageHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

@DriverSide
public class ElasticMemoryEvaluatorConfiguration {

  final NSWrapperDriver nsWrapperDriver;
  final NameServer nameServer;
  final IdentifierFactory ifac = new StringIdentifierFactory();
  final Set<String> contextIdSet = new HashSet<>();

  @Inject
  private ElasticMemoryEvaluatorConfiguration(final NSWrapperDriver nsWrapperDriver,
                                              final NameServer nameServer) {
    this.nsWrapperDriver = nsWrapperDriver;
    this.nameServer = nameServer;
  }

  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageHandlers.class, ElasticMemoryCtrlMsgHandler.class)
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
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStoreClient.class)
//        .set(ServiceConfiguration.ON_CONTEXT_STARTED, ElasticMemoryClient.class)
//        .set(ServiceConfiguration.ON_CONTEXT_STOP, UnBindNSWrapperToContext.class)
        .build();

    final Configuration bindConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStoreClient.class, ElasticMemoryStoreClient.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindConf);
  }
}
