package org.apache.reef.inmemory.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Constructs an instance of the Hadoop DFSClient, for use by Tang injector.
 */
public class DfsClientConstructor implements ExternalConstructor<DFSClient> {
  private final String dfsAddress;

  @Inject
  DfsClientConstructor(final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.dfsAddress = dfsAddress;
  }

  @Override
  public DFSClient newInstance() {
    try {
      return new DFSClient(new URI(this.dfsAddress), new Configuration());
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Failed to connect DFS Client in " + this.dfsAddress);
    }
  }
}
