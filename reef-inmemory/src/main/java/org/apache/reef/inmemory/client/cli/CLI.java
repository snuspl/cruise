package org.apache.reef.inmemory.client.cli;

import com.google.common.net.HostAndPort;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.reef.inmemory.client.SurfFS;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Command Line Interface for sending Cache management commands to the
 * InMemoryDriver.
 *
 * Give command with -cmd (command) option
 * - clear : Clear the task at the driver and all Tasks
 * - load : Load into the task the file on the given path
 *
 * Other options:
 * -hostname (hostname) : InMemory Cache driver hostname (default: localhost)
 * -port (port) : InMemory Cache driver port (default: 18000)
 * -path (path) : File path (required for load, no default)
 */
public final class CLI {

  private static final Logger LOG = Logger.getLogger(CLI.class.getName());

  @NamedParameter(doc = "Command", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  @NamedParameter(doc = "InMemory Cache driver address",
          short_name = "address", default_value = "yarn.reef-job-InMemory")
  public static final class Address implements Name<String> {
  }

  @NamedParameter(doc = "Underlying DFS address", short_name = "dfs_address", default_value = "hdfs://localhost:9000")
  public static final class DfsAddress implements Name<String> {
  }

  @NamedParameter(doc = "InMemory new Cache Server memory amount in MB",
          short_name = "cache_memory", default_value = "0")
  public static final class CacheServerMemory implements Name<Integer> {
  }

  @NamedParameter(doc = "DFS file path for load operation",
          short_name = "path")
  public static final class Path implements Name<String> {
  }

  @NamedParameter(doc = "Recursively apply path", short_name = "recursive", default_value = "false")
  public static final class Recursive implements Name<Boolean> {
  }

  public static SurfManagementService.Client getClient(String address)
          throws TTransportException {
    final HostAndPort metaAddress = HostAndPort.fromString(address);
    final TTransport transport = new TFramedTransport(new TSocket(metaAddress.getHostText(), metaAddress.getPort()));
    transport.open();
    final TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfManagementService.class.getName());
    return new SurfManagementService.Client(protocol);
  }

  private static SurfFS getFileSystem(final String address, final String dfsAddress) throws IOException {
    final SurfFS surfFs = new SurfFS();
    final org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, dfsAddress);
    surfFs.initialize(URI.create("surf://" + address), conf);
    return surfFs;
  }

  private static boolean runCommand(final Configuration config)
          throws InjectionException, TException, IOException {
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final String cmd = injector.getNamedInstance(Command.class);
    final String address = injector.getNamedInstance(Address.class);
    final String dfsAddress = injector.getNamedInstance(DfsAddress.class);
    final int cacheMemory = injector.getNamedInstance(CacheServerMemory.class);

    final SurfFS surfFs = getFileSystem(address, dfsAddress);
    final String rawAddress = surfFs.getMetaserverResolver().getAddress();
    final SurfManagementService.Client client = getClient(rawAddress);

    if ("status".equals(cmd)) {
      final String status = client.getStatus();
      LOG.log(Level.INFO, '\n'+status);
      return true;
    } else if ("clear".equals(cmd)) {
      final long numCleared = client.clear();
      LOG.log(Level.INFO, "Cleared {0} items from task", numCleared);
      return true;
    } else if ("load".equals(cmd)) {
      final String path = injector.getNamedInstance(Path.class);
      final boolean recursive = injector.getNamedInstance(Recursive.class);
      if (recursive) {
        final List<FileStatus> statuses = CLIUtils.getRecursiveList(surfFs, path);
        for (final FileStatus status : statuses) {
          client.load(status.getPath().toUri().getPath());
        }
        return true;
      } else {
        return client.load(path);
      }
    } else if ("addcache".equals(cmd)) {
      final String result = client.addCacheNode(cacheMemory);
      return true;
    } else if ("replication-list".equals(cmd)) {
      final String result = client.getReplication();
      LOG.log(Level.INFO, "\n"+result);
      return true;
    } else if ("replication-upload".equals(cmd)) {
      final String path = injector.getNamedInstance(Path.class);
      final File file = new File(path);
      String rules = FileUtils.readFileToString(file);
      return client.setReplication(rules);
    } else {
      return false;
    }
  }

  private static Configuration parseCommandLine(final String[] args)
          throws IOException {
    final JavaConfigurationBuilder confBuilder =
            Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder)
            .registerShortNameOfClass(Command.class)
            .registerShortNameOfClass(Address.class)
            .registerShortNameOfClass(DfsAddress.class)
            .registerShortNameOfClass(Path.class)
            .registerShortNameOfClass(Recursive.class)
            .registerShortNameOfClass(CacheServerMemory.class)
            .processCommandLine(args);
    return confBuilder.build();
  }

  public static void main(String args[])
          throws TException, IOException, InjectionException {
    final Configuration config = parseCommandLine(args);
    boolean success = runCommand(config);
    LOG.log(Level.INFO, success ? "Run command succeeded" : "Run command failed");
  }
}
