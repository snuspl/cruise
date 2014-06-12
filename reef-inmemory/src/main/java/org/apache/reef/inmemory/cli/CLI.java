package org.apache.reef.inmemory.cli;

import com.microsoft.tang.Configuration;
import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.annotations.Name;
import com.microsoft.tang.annotations.NamedParameter;
import com.microsoft.tang.exceptions.InjectionException;
import com.microsoft.tang.formats.CommandLine;
import org.apache.reef.inmemory.fs.service.SurfManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

/**
 * Command Line Interface for sending Cache management commands to the
 * InMemoryDriver.
 * <p>
 * Give command with -cmd &lt;command&gt; option
 * <ul>
 *   <li> clear : Clear the cache at the Driver and all Tasks </li>
 * </ul>
 * <p>
 * Other options:
 * <ul>
 *   <li> -hostname &lt;hostname&gt; :
 *                     InMemory Cache Driver hostname (default: localhost)</li>
 *   <li> -port &lt;port&gt; : InMemory Cache Driver port (default: 18000)</li>
 * </ul>
 */
public class CLI {

  @NamedParameter(doc = "Command", short_name = "cmd")
  public static final class Command implements Name<String> {
  }

  @NamedParameter(doc = "InMemory Cache Driver hostname",
          short_name = "hostname", default_value = "localhost")
  public static final class Hostname implements Name<String> {
  }

  @NamedParameter(doc = "InMemory Cache Driver port",
          short_name = "port", default_value = "18000")
  public static final class Port implements Name<Integer> {
  }

  private static SurfManagementService.Client getClient(String host, int port)
          throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfManagementService.class.getName());
    return new SurfManagementService.Client(protocol);
  }

  private static boolean runCommand(final Configuration config)
          throws InjectionException, TException {
    final Injector injector = Tang.Factory.getTang().newInjector(config);
    String cmd = injector.getNamedInstance(Command.class);
    String hostname = injector.getNamedInstance(Hostname.class);
    int port = injector.getNamedInstance(Port.class);

    if ("clear".equals(cmd)) {
      SurfManagementService.Client client = getClient(hostname, port);
      System.out.println("Connected to surf");
      long numCleared = client.clear();
      System.out.println("numCleared: "+numCleared);
      return true;
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
            .processCommandLine(args);
    return confBuilder.build();
  }

  public static void main(String args[])
          throws TException, IOException, InjectionException {
    final Configuration config = parseCommandLine(args);
    runCommand(config);
  }
}
