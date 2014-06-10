package org.apache.reef.inmemory.cli;

import org.apache.commons.cli.*;

public class CLI {

  private static Options getOptions() {
    Options options = new Options()
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
    return options;
  }

  private static void runCommand(String command, CommandLine line) {
    // TODO: implement commands
    System.out.println("Command: "+command);
    for (Option option: line.getOptions()) {
      System.out.println(option.getArgName()+" : "+option.getValue());
    }
  }

  public static void main(String args[]) throws ParseException {
    CommandLineParser parser = new PosixParser();
    Options options = getOptions();
    CommandLine line = parser.parse(options, args);
    if (line.getArgs().length == 1) {
      runCommand(line.getArgs()[0], line);
    } else {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("surf command", options, true);
    }
  }
}
