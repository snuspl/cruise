/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.dnn;

import org.apache.commons.cli.*;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.NameResolutionException;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.tang.types.Node;
import org.apache.reef.tang.util.MonotonicSet;
import org.apache.reef.tang.util.MonotonicTreeMap;
import org.apache.reef.tang.util.ReflectionUtilities;

import java.io.IOException;
import java.util.*;

/**
 * Command line parser that parses registered {@link org.apache.reef.tang.annotations.NamedParameter} options.
 * This class is similar to {@link org.apache.reef.tang.formats.CommandLine}.
 * However, this can ignore unrecognized options and parse only the registered named parameter options.
 * The ignored options and command line arguments can be provided
 * through {@code getRemainArgList()} and {@code getRemainArgs()}.
 */
public final class CommandLineFilter {

  private final ConfigurationBuilder conf;
  private final Map<String, String> shortNames = new MonotonicTreeMap<>();
  private final List<String> remainArgs = new ArrayList<>();
  private final Set<String> filteredShortNames = new MonotonicSet<>();

  public CommandLineFilter() {
    this.conf = Tang.Factory.getTang().newConfigurationBuilder();
  }

  public CommandLineFilter(final ConfigurationBuilder conf) {
    this.conf = conf;
  }

  public ConfigurationBuilder getBuilder() {
    return this.conf;
  }

  public CommandLineFilter registerShortNameOfClass(final String s) {
    return registerShortNameOfClass(s, true);
  }

  public CommandLineFilter registerShortNameOfClass(final String s, final boolean filtered) throws BindException {

    final Node n;
    try {
      n = conf.getClassHierarchy().getNode(s);
    } catch (final NameResolutionException e) {
      throw new BindException("Problem loading class " + s, e);
    }

    if (n instanceof NamedParameterNode) {
      final NamedParameterNode<?> np = (NamedParameterNode<?>) n;
      final String shortName = np.getShortName();
      final String longName = np.getFullName();
      if (shortName == null) {
        throw new BindException(
            "Can't register non-existent short name of named parameter: " + longName);
      }
      shortNames.put(shortName, longName);
      if (filtered) {
        filteredShortNames.add(shortName);
      }
    } else {
      throw new BindException("Can't register short name for non-NamedParameterNode: " + n);
    }

    return this;
  }

  public CommandLineFilter registerShortNameOfClass(
      final Class<? extends Name<?>> c) throws BindException {
    return registerShortNameOfClass(ReflectionUtilities.getFullName(c));
  }

  public CommandLineFilter registerShortNameOfClass(
      final Class<? extends Name<?>> c, final boolean filtered) throws BindException {
    return registerShortNameOfClass(ReflectionUtilities.getFullName(c), filtered);
  }

  @SuppressWarnings("static-access")
  private Options getCommandLineOptions() {

    final Options opts = new Options();
    for (final Map.Entry<String, String> entry : shortNames.entrySet()) {
      final String shortName = entry.getKey();
      final String longName = entry.getValue();
      try {
        opts.addOption(OptionBuilder
            .withArgName(conf.classPrettyDefaultString(longName)).hasArg()
            .withDescription(conf.classPrettyDescriptionString(longName))
            .create(shortName));
      } catch (final BindException e) {
        throw new IllegalStateException(
            "Could not process " + shortName + " which is the short name of " + longName, e);
      }
    }

    return opts;
  }

  @SafeVarargs
  public final CommandLineFilter processCommandLine(
      final String[] args, final Class<? extends Name<?>>... argClasses)
      throws IOException, BindException {

    for (final Class<? extends Name<?>> c : argClasses) {
      registerShortNameOfClass(c);
    }

    final Options o = getCommandLineOptions();
    o.addOption(new Option("?", "help"));

    String[] currentArgs = args;
    while (currentArgs.length > 0) {
      final Parser g = new GnuParser();
      final org.apache.commons.cli.CommandLine cl;
      try {
        cl = g.parse(o, currentArgs, true);
      } catch (final ParseException e) {
        throw new IOException("Could not parse config file", e);
      }

      if (cl.hasOption("?")) {
        new HelpFormatter().printHelp("reef", o);
        return null;
      }

      for (final Option option : cl.getOptions()) {
        final String shortName = option.getOpt();
        final String value = option.getValue();

        try {
          conf.bind(shortNames.get(shortName), value);
        } catch (final BindException e) {
          throw new BindException("Could not bind shortName " + shortName + " to value " + value, e);
        }

        if (!filteredShortNames.contains(shortName)) {
          remainArgs.add("-" + shortName);
          remainArgs.add(value);
        }
      }

      currentArgs = cl.getArgs();
      if (currentArgs.length > 0) {
        // there was an unrecognized option.
        remainArgs.add(currentArgs[0]);
        currentArgs = Arrays.copyOfRange(currentArgs, 1, currentArgs.length);
      }
    }
    return this;
  }

  public List<String> getRemainArgList() {
    return this.remainArgs;
  }

  public String[] getRemainArgs() {
    final String[] args = new String[remainArgs.size()];
    remainArgs.toArray(args);
    return args;
  }
}
