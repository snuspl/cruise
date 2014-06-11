package org.apache.reef.inmemory.cli;

import org.apache.commons.cli.*;
import org.apache.reef.inmemory.fs.service.SurfManagementService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CLI {

  private static Options getOptions() {
    return new Options()
      .addOption(
        OptionBuilder.withArgName("host")
              .hasArg()
              .withDescription("Cache server host address")
              .create("host")
      )
      .addOption(
              OptionBuilder.withArgName("port")
                      .hasArg()
                      .withDescription("Cache server port number")
                      .create("port")
      );
  }

  private static SurfManagementService.Client getClient(String host, int port) throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport), SurfManagementService.class.getName());
    return new SurfManagementService.Client(protocol);
  }

  private static boolean runCommand(String command, CommandLine line) throws TException {
    System.out.println("Command: "+command);
    for (Option option: line.getOptions()) {
      System.out.println(option.getArgName()+" : "+option.getValue());
    }
    if ("clear".equals(command)) {
      SurfManagementService.Client client = getClient(line.getOptionValue("host", "localhost"),
              Integer.parseInt(line.getOptionValue("port", "18000")));
      System.out.println("Connected to surf");
      long numCleared = client.clear();
      System.out.println("numCleared: "+numCleared);
      return true;
    } else {
      return false;
    }
  }

  public static void main(String args[]) throws ParseException, TTransportException {
    CommandLineParser parser = new PosixParser();
    Options options = getOptions();
    CommandLine line = parser.parse(options, args);
    if (line.getArgs().length == 1) {
      try {
        if (!runCommand(line.getArgs()[0], line)) {
          System.err.println("Unknown command: " + line.getArgs()[0]);
        }
      } catch (TException e) {
        System.err.print("Unexpected exception: ");
        e.printStackTrace();
      }
    } else {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("surf command", options, true);
    }
  }
}
