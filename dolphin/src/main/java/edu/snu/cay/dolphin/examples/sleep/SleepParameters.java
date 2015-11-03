/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.sleep;

import edu.snu.cay.dolphin.core.UserParameters;
import edu.snu.cay.dolphin.examples.ml.parameters.MaxIterations;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.driver.parameters.DriverLocalFiles;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;
import java.io.*;

/**
 * {@link UserParameters} for the SleepREEF application.
 *
 * <p>
 *   A configuration file that specifies the initial workloads and
 *   computation speeds of evaluators is read and parsed at the driver.
 *   The driver then binds these values to named parameters and submits them
 *   when creating tasks. The configuration file does not necessarily have to
 *   be on a distributed file system even when running on YARN or Mesos.
 * </p>
 *
 * <p>
 *   Following is an example of a configuration file.
 *   The first column represents the initial number of data units (workload) given to an evaluator.
 *   The second column is the computation speed of an evaluator (ms per data unit).
 *   When the number of evaluators start to exceed the number of lines,
 *   the default values (0, 1.0) are assigned. Don't leave any blank lines.
 * </p>
 *
 * <pre>{@code
 *   100 1.0
 *   200 1.5
 *   300 0.8
 *   0 1.0
 *   0 1.2
 * }</pre>
 */
public final class SleepParameters implements UserParameters {

  public static final String KEY = "KEY";

  private final File confFile;
  private final int maxIterations;
  private BufferedReader bufferedReader;

  @Inject
  private SleepParameters(@Parameter(ConfigurationFilePath.class) final String confFilePath,
                          @Parameter(MaxIterations.class) final int maxIterations) {
    this.confFile = new File(confFilePath);
    this.maxIterations = maxIterations;
  }

  /**
   * The driver-side configuration for SleepREEF.
   * The local configuration file is passed to the container for the driver.
   */
  @Override
  public Configuration getDriverConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindSetEntry(DriverLocalFiles.class, confFile.getAbsolutePath())
        .bindNamedParameter(ConfigurationFilePath.class, String.format("reef/local/%s", confFile.getName()))
        .bindNamedParameter(MaxIterations.class, Integer.toString(maxIterations))
        .build();
  }

  /**
   * The service configuration for SleepREEF compute tasks and the SleepREEF controller task.
   */
  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Serializer.class, SleepSerializer.class)
        .build();
  }

  /**
   * The configuration for a SleepREEF compute task.
   * This needs to be {@code synchronized} because each line of
   * the conf file must be assigned to only one evaluator.
   */
  @Override
  public synchronized Configuration getUserCmpTaskConf() {
    final String initialWorkloadStr;
    final String computationSpeedStr;

    try {
      // open the conf file if isn't already opened
      if (bufferedReader == null) {
        bufferedReader = new BufferedReader(new FileReader(confFile));
      }

      final String str = bufferedReader.readLine();
      if (str == null) {
        // default values in case of EOF
        initialWorkloadStr = "0";
        computationSpeedStr = "1.0";

      } else {
        final String[] args = str.trim().split("\\s+");

        initialWorkloadStr = args[0];
        final int initialWorkload = Integer.parseInt(initialWorkloadStr);
        if (initialWorkload < 0) {
          throw new RuntimeException(String.format("Initial workload is negative %d", initialWorkload));
        }

        computationSpeedStr = args[1];
        final float computationSpeed = Float.parseFloat(computationSpeedStr);
        if (computationSpeed < 0) {
          throw new RuntimeException(String.format("Computation speed is negative %f", computationSpeed));
        }
      }

    } catch (final FileNotFoundException e) {
      throw new RuntimeException(String.format("No file named %s", confFile), e);
    } catch (final IOException e) {
      throw new RuntimeException(String.format("Failed to read line from %s", confFile), e);
    } catch (final NumberFormatException e) {
      throw new RuntimeException(String.format("Failed to parse integer or float from %s", confFile), e);
    }


    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InitialWorkload.class, initialWorkloadStr)
        .bindNamedParameter(ComputationSpeed.class, computationSpeedStr)
        .build();
  }

  /**
   * The configuration for the SleepREEF controller task.
   */
  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MaxIterations.class, Integer.toString(maxIterations))
        .build();
  }

  /**
   * Read the configuration file path and the number of iterations via command line.
   */
  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(ConfigurationFilePath.class);
    cl.registerShortNameOfClass(MaxIterations.class);
    return cl;
  }

  @NamedParameter(doc = "input path of the configuration file", short_name = "conf")
  private final class ConfigurationFilePath implements Name<String> {
  }

  @NamedParameter(doc = "initial number of data units given to an evaluator")
  final class InitialWorkload implements Name<Integer> {
  }

  @NamedParameter(doc = "the computation rate of an evaluator, milliseconds per data unit")
  final class ComputationSpeed implements Name<Float> {
  }
}
