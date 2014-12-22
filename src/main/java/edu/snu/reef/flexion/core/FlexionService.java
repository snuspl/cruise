package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.api.task.GroupCommClient;
import org.apache.reef.driver.context.ServiceConfiguration;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;

import javax.inject.Inject;
import java.util.logging.Logger;

public final class FlexionService {
  private final static Logger LOG = Logger.getLogger(FlexionService.class.getName());

  private final FlexionCommunicator flexionCommunicator;

  @Inject
  FlexionService(final GroupCommClient groupCommClient) {
    this.flexionCommunicator = new FlexionServiceCtrl(groupCommClient);
  }

  @Inject
  FlexionService(final DataSet dataSet,
                 final GroupCommClient groupCommClient) {
    this.flexionCommunicator = new FlexionServiceCmp(groupCommClient);
  }

  public final static Configuration getServiceConfiguration() {
    return ServiceConfiguration.CONF
        .set(ServiceConfiguration.SERVICES, FlexionService.class)
        .build();
  }

  public final void send(final Integer data) throws Exception {
    flexionCommunicator.send(data);
  }

  public final Integer recieve() throws Exception {
    return flexionCommunicator.receive();
  }

  public final boolean terminate() throws Exception {
    return flexionCommunicator.terminate();
  }
}
