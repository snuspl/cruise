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
package edu.snu.cay.services.et.examples.simple;

import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client code for simple example.
 */
public final class SimpleET {
  private static final Logger LOG = Logger.getLogger(SimpleET.class.getName());

  private static final String DRIVER_IDENTIFIER = "Simple";
  private static final int MAX_NUMBER_OF_EVALUATORS = 4; // 1 driver + 2 associator + 1 subscriber
  private static final int JOB_TIMEOUT = 30000; // 30 sec.

  /**
   * Should not be instantiated.
   */
  private SimpleET() {
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final Configuration clConf = parseCommandLine(args);
    final String tableInput = Tang.Factory.getTang().newInjector(clConf)
        .getNamedInstance(TableInputPath.class);

    final LauncherStatus status = runSimpleET(tableInput);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(TableInputPath.class).processCommandLine(args);
    return cb.build();
  }

  private static String processInputDir(final String inputDir) throws InjectionException {
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }

  /**
   * Runs SimpleET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  public static LauncherStatus runSimpleET(final String tableInputPath) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SimpleETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, SimpleETDriver.StartHandler.class)
        .build();
    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    final String processedTableInput = tableInputPath.equals(TableInputPath.EMPTY) ?
        tableInputPath : processInputDir(tableInputPath);

    final Configuration exampleConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(TableInputPath.class, processedTableInput)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration, exampleConfiguration), JOB_TIMEOUT);
  }

  @NamedParameter(doc = "Path of a input file to load on a table",
      short_name = "table_input", default_value = TableInputPath.EMPTY)
  public final class TableInputPath implements Name<String> {
    public static final String EMPTY = "";

    // should not be instantiated
    private TableInputPath() {

    }
  }
}
