package edu.snu.cay.em.driver;

import edu.snu.cay.em.evaluator.impl.ElasticMemoryStore;
import edu.snu.cay.em.msg.ElasticMemoryMsgCodec;
import edu.snu.cay.em.ns.NSWrapperConfiguration;
import edu.snu.cay.em.ns.NSWrapperContextRegister;
import edu.snu.cay.em.ns.NSWrapperParameters;
import edu.snu.cay.em.evaluator.api.MemoryStore;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextStartHandlers;
import org.apache.reef.evaluator.context.parameters.ContextStopHandlers;
import org.apache.reef.io.network.group.impl.driver.ExceptionHandler;
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
   * Configuration for REEF driver when using Elastic Memory.
   * Binds named parameters for NSWrapper, excluding NameServer-related and default ones.
   * The NameServer will be instantiated at the driver by Tang, and thus NameServer
   * parameters (namely address and port) will be set at runtime by receiving
   * a NameServer injection from Tang.
   *
   * @return configuration that should be submitted with a DriverConfiguration
   */
  public static Configuration getDriverConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NSWrapperParameters.NetworkServiceCodec.class, ElasticMemoryMsgCodec.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServiceHandler.class, ElasticMemoryMsgHandler.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServiceExceptionHandler.class, ExceptionHandler.class)
        .bindNamedParameter(NSWrapperParameters.NetworkServicePort.class, "0")
        .build();
  }

  /**
   * Configuration for REEF context with Elastic Memory.
   * Elastic Memory requires contexts that communicate through NSWrapper.
   * This configuration binds handlers that register contexts to / unregister
   * contexts from NSWrapper.
   *
   * @return configuration that should be merged with a ContextConfiguration to form a context
   */
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextStartHandlers.class, NSWrapperContextRegister.RegisterContextHandler.class)
        .bindSetEntry(ContextStopHandlers.class, NSWrapperContextRegister.UnregisterContextHandler.class)
        .build();
  }

  /**
   * Configuration for REEF service with Elastic Memory.
   * Sets up NSWrapper and ElasticMemoryStore, both required for Elastic Memory.
   *
   * @return service configuration that should be passed along with a ContextConfiguration
   */
  public Configuration getServiceConfiguration() {
    final Configuration nsWrapperConf =
        nsWrapperConfiguration.getConfiguration(ElasticMemoryMsgCodec.class,
                                                edu.snu.cay.em.evaluator.ElasticMemoryMsgHandler.class);

    final Configuration serviceConf = ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, ElasticMemoryStore.class)
        .build();

    final Configuration bindImplConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(MemoryStore.class, ElasticMemoryStore.class)
        .build();

    return Configurations.merge(nsWrapperConf, serviceConf, bindImplConf);
  }
}
