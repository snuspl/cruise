package org.apache.reef.inmemory.client;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class YarnMetaserverResolver implements MetaserverResolver {

  private static final Logger LOG = Logger.getLogger(YarnMetaserverResolver.class.getName());

  private final String identifier;
  private final Configuration conf;

  public YarnMetaserverResolver(final String identifier,
                                final Configuration conf) {
    this.identifier = identifier;
    this.conf = conf;
  }

  private String getTrackingUrl() throws IOException {
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
          return app.getOriginalTrackingUrl(); // getTrackingUrl() gives a bad host name on local Mac OS
        }
      }
      throw new IOException("Could not find application "+jobName);
    } catch (YarnException e) {
      throw new IOExceptionWithCause("Could not find application "+jobName, e);
    }
  }

  @Override
  public String getAddress() throws IOException {
    final HttpClient client = new DefaultHttpClient();
    LOG.log(Level.INFO, "http://"+getTrackingUrl()+"/surf/v1");
    final HttpGet request = new HttpGet("http://"+getTrackingUrl()+"/surf/v1");
    final HttpResponse response = client.execute(request);
    return IOUtils.toString(response.getEntity().getContent());
  }
}
