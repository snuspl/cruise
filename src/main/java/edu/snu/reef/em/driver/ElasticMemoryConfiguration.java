package edu.snu.reef.em.driver;

import edu.snu.reef.em.evaluator.api.MemoryStore;
import edu.snu.reef.em.evaluator.impl.ElasticMemoryStore;
import edu.snu.reef.em.msg.ElasticMemoryMsgCodec;
import edu.snu.reef.em.msg.ElasticMemoryMsgBroadcaster;
import edu.snu.reef.em.ns.NSWrapperConfiguration;
import edu.snu.reef.em.ns.NSWrapperContextRegister;
import edu.snu.reef.em.evaluator.*;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;

import javax.inject.Inject;

/**
 * Configuration class for setting evaluator configurations of ElasticMemoryService.
 */
@DriverSide
public final class ElasticMemoryConfiguration {

  private final NSWrapperConfiguration nsWrapperConfiguration;

  @Inject
  private ElasticMemoryConfiguration(final NSWrapperConfiguration nsWrapperConfiguration) {
    this.nsWrapperConfiguration = nsWrapperConfiguration;
  }

  /**
   * @return configuration that should be merged with a ContextConfiguration to form a context
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, ElasticMemoryMsgHandlerBinder.ContextStartHandler.class)
        .bindSetEntry(ContextStartHandlers.class, NSWrapperContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, ElasticMemoryMsgHandlerBinder.ContextStopHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NSWrapperContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * @return service configuration that should be passed along with a ContextConfiguration
   */
  public Configuration getServiceConfiguration() {
    final Configuration nsWrapperConf =
        nsWrapperConfiguration.getConfiguration(ElasticMemoryMsgCodec.class,
                                                ElasticMemoryMsgBroadcaster.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStore.class)
        .build();

    final Configuration bindImplConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, ElasticMemoryStore.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindImplConf);
  }
}
