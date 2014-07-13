package org.apache.reef.inmemory.client;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

public final class YarnMetaserverResolver implements MetaserverResolver {

  private final String identifier;
  private final Configuration conf;

  public YarnMetaserverResolver(final String identifier,
                                final Configuration conf) {
    this.identifier = identifier;
    this.conf = conf;
  }

  @Override
  public String getAddress() throws IOException {
    final String jobName = identifier.substring("yarn.".length());
    final ApplicationClientProtocol appClient =
            ClientRMProxy.createRMProxy(conf, ApplicationClientProtocol.class);

    try {
      final GetApplicationsRequest request = GetApplicationsRequest.newInstance(
              EnumSet.of(YarnApplicationState.RUNNING));

      final GetApplicationsResponse response = appClient.getApplications(request);
      final List<ApplicationReport> apps = response.getApplicationList();
      for (ApplicationReport app : apps) {
        if (jobName.equals(app.getName())) {
          return app.getTrackingUrl();
        }
      }
      throw new IOException("Could not find application "+jobName);
    } catch (YarnException e) {
      throw new IOExceptionWithCause("Could not find application "+jobName, e);
    }
  }
}
