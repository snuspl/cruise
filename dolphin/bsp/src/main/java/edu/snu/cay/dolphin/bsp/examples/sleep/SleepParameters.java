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
package edu.snu.cay.dolphin.bsp.examples.sleep;

import edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.dolphin.bsp.core.UserParameters;
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
 *   The user must provide a configuration file that specifies the
 *   initial workloads and computation rates of evaluators. This file
 *   is read and parsed at the driver. The driver then binds these
 *   values to named parameters and submits them when creating tasks.
 *   The configuration file does not necessarily have to
 *   be on a distributed file system even when running on YARN or Mesos.
 *
 *   Other parameters such as the size of a single serialized object, and
 *   encode/decode rates are also passed to evaluators here.
 * </p>
 *
 * <p>
 *   Following is an example of a configuration file.
 *   The first column represents the initial number of data units (workload) given to an evaluator.
 *   The second column is the computation rate of an evaluator (ms per data unit).
 *   When the number of evaluators start to exceed the number of lines,
 *   the default values (0, 50) are assigned.
 * </p>
 *
 * <pre>{@code
 *   75 52
 *   120 53
 *   30 49
 *   0 50
 *   0 51
 * }</pre>
 */
public final class SleepParameters implements UserParameters {

  public static final String KEY = "KEY";

  private final File confFile;
  private final int maxIterations;
  private final long gcEncodeTime;
  private final long gcDecodeTime;
  private final long emEncodeRate;
  private final long emDecodeRate;
  private BufferedReader bufferedReader;

  @Inject
  private SleepParameters(@Parameter(ConfigurationFilePath.class) final String confFilePath,
                          @Parameter(Iterations.class) final int maxIterations,
                          @Parameter(GCEncodeTime.class) final long gcEncodeTime,
                          @Parameter(GCDecodeTime.class) final long gcDecodeTime,
                          @Parameter(EMEncodeRate.class) final long emEncodeRate,
                          @Parameter(EMDecodeRate.class) final long emDecodeRate) {
    this.confFile = new File(confFilePath);
    this.maxIterations = maxIterations;
    this.gcEncodeTime = gcEncodeTime;
    this.gcDecodeTime = gcDecodeTime;
    this.emEncodeRate = emEncodeRate;
    this.emDecodeRate = emDecodeRate;
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
        .bindNamedParameter(Iterations.class, Integer.toString(maxIterations))
        .bindNamedParameter(GCEncodeTime.class, Long.toString(gcEncodeTime))
        .bindNamedParameter(GCDecodeTime.class, Long.toString(gcDecodeTime))
        .bindNamedParameter(EMEncodeRate.class, Long.toString(emEncodeRate))
        .bindNamedParameter(EMDecodeRate.class, Long.toString(emDecodeRate))
        .build();
  }

  /**
   * The service configuration for SleepREEF compute tasks and the SleepREEF controller task.
   */
  @Override
  public Configuration getServiceConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(Serializer.class, SleepSerializer.class)
        .bindNamedParameter(GCEncodeTime.class, Long.toString(gcEncodeTime))
        .bindNamedParameter(GCDecodeTime.class, Long.toString(gcDecodeTime))
        .bindNamedParameter(EMEncodeRate.class, Long.toString(emEncodeRate))
        .bindNamedParameter(EMDecodeRate.class, Long.toString(emDecodeRate))
        .build();
  }

  /**
   * Returns a single line from the given configuration file.
   *
   * The '#' character and other characters following it are regarded as
   * inline comments and are removed, as well as leading and trailing whitespaces.
   * In case the processed string is empty, this method skips that line and
   * repeats until it finds a non-empty result string.
   *
   * This method is declared {@code synchronized} to prevent concurrent reads;
   * each caller should take a different line.
   *
   * @return a processed line from the given conf file, or {@code null} if EOF
   */
  private synchronized String readLineFromFile() {
    try {
      if (bufferedReader == null) {
        bufferedReader = new BufferedReader(new FileReader(confFile));
      }

      String str;
      do {
        str = bufferedReader.readLine();
        if (str == null) {
          return null;
        }

        final int indexOfComments = str.indexOf('#');
        if (indexOfComments != -1) {
          str = str.substring(0, indexOfComments);
        }
        str = str.trim();

      } while (str.isEmpty());
      return str;

    } catch (final FileNotFoundException e) {
      throw new RuntimeException(String.format("No file named %s", confFile), e);
    } catch (final IOException e) {
      throw new RuntimeException(String.format("Failed to read line from %s", confFile), e);
    }
  }

  /**
   * The configuration for a SleepREEF compute task.
   * This is called at the driver, once for each compute task.
   * The returned configuration contains named parameter configurations for
   * {@link InitialWorkload} and {@link ComputationRate}.
   */
  @Override
  public Configuration getUserCmpTaskConf() {
    final String initialWorkloadStr;
    final String computationRateStr;

    final String str = readLineFromFile();
    if (str == null) {
      // in case of EOF, return empty configuration
      return Tang.Factory.getTang().newConfigurationBuilder().build();

    } else {
      final String[] args = str.split("\\s+");

      initialWorkloadStr = args[0];
      final int initialWorkload = Integer.parseInt(initialWorkloadStr);
      if (initialWorkload < 0) {
        throw new RuntimeException(String.format("Initial workload is negative %d", initialWorkload));
      }

      computationRateStr = args[1];
      final long computationRate = Long.parseLong(computationRateStr);
      if (computationRate < 0) {
        throw new RuntimeException(String.format("Computation rate is negative %d", computationRate));
      }
    }

    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InitialWorkload.class, initialWorkloadStr)
        .bindNamedParameter(ComputationRate.class, computationRateStr)
        .build();
  }

  /**
   * The configuration for the SleepREEF controller task.
   */
  @Override
  public Configuration getUserCtrlTaskConf() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(Iterations.class, Integer.toString(maxIterations))
        .build();
  }

  /**
   * Read the configuration file path and the number of iterations via command line.
   */
  public static CommandLine getCommandLine() {
    final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);
    cl.registerShortNameOfClass(ConfigurationFilePath.class);
    cl.registerShortNameOfClass(Iterations.class);
    cl.registerShortNameOfClass(GCEncodeTime.class);
    cl.registerShortNameOfClass(GCDecodeTime.class);
    cl.registerShortNameOfClass(EMEncodeRate.class);
    cl.registerShortNameOfClass(EMDecodeRate.class);
    return cl;
  }

  @NamedParameter(doc = "input path of the configuration file", short_name = "conf")
  private final class ConfigurationFilePath implements Name<String> {
  }

  @NamedParameter(doc = "initial number of data units given to an evaluator", default_value = "0")
  final class InitialWorkload implements Name<Integer> {
  }

  @NamedParameter(doc = "the computation rate of an evaluator, milliseconds per data unit", default_value = "50")
  final class ComputationRate implements Name<Long> {
  }

  @NamedParameter(doc = "time required to encode data per operation/iteration for group communication, milliseconds",
                  short_name = "gcEncodeTime",
                  default_value = "0")
  final class GCEncodeTime implements Name<Long> {
  }

  @NamedParameter(doc = "time required to decode data per operation/iteration for group communication, milliseconds",
                  short_name = "gcDecodeTime",
                  default_value = "0")
  final class GCDecodeTime implements Name<Long> {
  }

  @NamedParameter(doc = "the encode rate of an evaluator for elastic memory, milliseconds per data unit",
                  short_name = "emEncodeRate",
                  default_value = "0")
  final class EMEncodeRate implements Name<Long> {
  }

  @NamedParameter(doc = "the decode rate of an evaluator for elastic memory, milliseconds per data unit",
                  short_name = "emDecodeRate",
                  default_value = "0")
  final class EMDecodeRate implements Name<Long> {
  }
}
