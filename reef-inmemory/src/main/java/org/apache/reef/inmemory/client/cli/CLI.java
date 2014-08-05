package org.apache.reef.inmemory.client.cli;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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

  @NamedParameter(doc = "InMemory Cache driver hostname",
          short_name = "hostname", default_value = "localhost")
  public static final class Hostname implements Name<String> {
  }

  @NamedParameter(doc = "InMemory Cache driver port",
          short_name = "port", default_value = "18000")
  public static final class Port implements Name<Integer> {
  }

  @NamedParameter(doc = "InMemory new Cache Server memory amount in MB",
          short_name = "cache_memory", default_value = "0")
  public static final class CacheServerMemory implements Name<Integer> {
  }

  @NamedParameter(doc = "DFS file path for load operation",
          short_name = "path")
  public static final class Path implements Name<String> {
  }

  private static SurfManagementService.Client getClient(String host, int port)
          throws TTransportException {
    final TTransport transport = new TFramedTransport(new TSocket(host, port));
    transport.open();
    final TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfManagementService.class.getName());
    return new SurfManagementService.Client(protocol);
  }

  private static boolean runCommand(final Configuration config)
          throws InjectionException, TException, IOException {
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    final String cmd = injector.getNamedInstance(Command.class);
    final String hostname = injector.getNamedInstance(Hostname.class);
    final int port = injector.getNamedInstance(Port.class);
    final int cacheMemory = injector.getNamedInstance(CacheServerMemory.class);
    final SurfManagementService.Client client = getClient(hostname, port);

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
      return client.load(path);
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
            .registerShortNameOfClass(Hostname.class)
            .registerShortNameOfClass(Port.class)
            .registerShortNameOfClass(Path.class)
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
