package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.reef.inmemory.common.DfsParameters;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;

/**
 * Constructs an instance of the {@link org.apache.hadoop.fs.FileSystem}, for use by Tang injector.
 */
public class DfsConstructor implements ExternalConstructor<FileSystem> {
  private final String dfsAddress;

  @Inject
  public DfsConstructor(final @Parameter(DfsParameters.Address.class) String dfsAddress) {
    this.dfsAddress = dfsAddress;
  }

  @Override
  public FileSystem newInstance() {
    try {
      final URI nameNodeUri = NameNode.getUri(NameNode.getAddress(this.dfsAddress));
      final FileSystem dfs = FileSystem.get(nameNodeUri, new Configuration());
      return dfs;
    } catch (IOException e) {
      throw new RuntimeException("Failed to connect DFS Client in " + this.dfsAddress);
    }
  }
}
