/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.examples.load;

import edu.snu.cay.common.client.DriverLauncher;
import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import org.apache.reef.client.DriverConfiguration;
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
 * Client code for Load ET example.
 */
public final class LoadET {
  private static final Logger LOG = Logger.getLogger(LoadET.class.getName());

  private static final String DRIVER_IDENTIFIER = "Load";

  private static final int MAX_NUMBER_OF_EVALUATORS = 3;
  private static final int JOB_TIMEOUT = 60000; // 60 sec.

  /**
   * Should not be instantiated.
   */
  private LoadET() {

  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final LauncherStatus status = runLoadET(args);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  private static Configuration parseCommandLine(final String[] args) throws IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(KeyValueDataPath.class);
    cl.registerShortNameOfClass(NoneKeyDataPath.class);
    cl.processCommandLine(args);
    return cb.build();
  }

  private static String processInputDir(final String inputDir) {
    final File inputFile = new File(inputDir);
    return "file:///" + inputFile.getAbsolutePath();
  }

  /**
   * Runs LoadET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  public static LauncherStatus runLoadET(final String[] args) throws InjectionException, IOException {


    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, MAX_NUMBER_OF_EVALUATORS)
        .build();

    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(LoadETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, LoadETDriver.StartHandler.class)
        .build();

    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();


    final Injector commandLineInjector = Tang.Factory.getTang().newInjector(parseCommandLine(args));
    final String keyInputPath = commandLineInjector.getNamedInstance(KeyValueDataPath.class);
    final String noneKeyInputPath = commandLineInjector.getNamedInstance(NoneKeyDataPath.class);

    final Configuration inputPathConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(KeyValueDataPath.class, processInputDir(keyInputPath))
        .bindNamedParameter(NoneKeyDataPath.class, processInputDir(noneKeyInputPath))
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration, inputPathConfiguration), JOB_TIMEOUT);
  }

  @NamedParameter(doc = "Path of a input file which has keys and values to load on a table",
      short_name = "kv_data_path")
  final class KeyValueDataPath implements Name<String> {

    // should not be instantiated
    private KeyValueDataPath() {

    }
  }

  @NamedParameter(doc = "Path of a input file which has only values to load on a table",
      short_name = "none_key_data_path")
  final class NoneKeyDataPath implements Name<String> {

    // should not be instantiated
    private NoneKeyDataPath() {

    }
  }
}
