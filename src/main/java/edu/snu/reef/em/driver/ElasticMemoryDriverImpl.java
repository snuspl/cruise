package edu.snu.reef.em.driver;

import edu.snu.reef.em.msg.ElasticMemoryCtrlMsgHandler;
import org.apache.reef.evaluator.context.parameters.ContextMessageHandlers;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;

public final class ElasticMemoryDriverImpl implements ElasticMemoryDriver {
  public Configuration getContextConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(ContextMessageHandlers.class, ElasticMemoryCtrlMsgHandler.class)
        .build();
  }

  public Configuration getServiceConfiguration() {
    return null;
  }
}
