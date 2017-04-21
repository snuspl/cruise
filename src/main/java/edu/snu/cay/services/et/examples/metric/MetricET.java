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
package edu.snu.cay.services.et.examples.metric;

import edu.snu.cay.services.et.configuration.ETDriverConfiguration;
import edu.snu.cay.services.et.configuration.metric.MetricServiceDriverConf;
import edu.snu.cay.services.et.driver.impl.LoggingMetricReceiver;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.metric.MetricETDriver.NUM_ASSOCIATORS;

/**
 * Client code for Metric example.
 */
public final class MetricET {
  private static final Logger LOG = Logger.getLogger(MetricET.class.getName());
  private static final String DRIVER_IDENTIFIER = "MetricET";
  private static final int JOB_TIMEOUT = 30000; // 30 sec.

  /**
   * Should not be instantiated.
   */
  private MetricET() {
  }

  public static void main(final String[] args) throws InjectionException {
    final LauncherStatus status = runMetricET(args);
    LOG.log(Level.INFO, "ET job completed: {0}", status);
  }

  private static Configuration parseCommandLine(final String[] args) {
    try {
      final ConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      final CommandLine cl = new CommandLine(cb);
      cl.registerShortNameOfClass(MetricAutomaticFlushPeriodMs.class)
          .registerShortNameOfClass(MetricManualFlushPeriodMs.class)
          .registerShortNameOfClass(CustomMetricRecordPeriodMs.class)
          .registerShortNameOfClass(TaskDurationMs.class);
      cl.processCommandLine(args);

      return cb.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Demonstrates how users can bind custom MetricReceiver implementation on the Driver-side.
   */
  private static Configuration getMetricServiceDriverConf() {
    return MetricServiceDriverConf.CONF
        .set(MetricServiceDriverConf.METRIC_RECEIVER_IMPL, LoggingMetricReceiver.class)
        .build();
  }

  /**
   * Runs MetricET example app.
   * @throws InjectionException when fail to inject DriverLauncher
   */
  private static LauncherStatus runMetricET(final String[] args) throws InjectionException {
    final Configuration runtimeConfiguration = LocalRuntimeConfiguration.CONF
        .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, NUM_ASSOCIATORS)
        .build();
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(MetricETDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, DRIVER_IDENTIFIER)
        .set(DriverConfiguration.ON_DRIVER_STARTED, MetricETDriver.StartHandler.class)
        .build();
    final Configuration cmdLineConf = parseCommandLine(args);
    final Configuration etMasterConfiguration = ETDriverConfiguration.CONF.build();
    final Configuration metricServiceDriverConf = getMetricServiceDriverConf();
    final Configuration nameServerConfiguration = NameServerConfiguration.CONF.build();
    final Configuration nameClientConfiguration = LocalNameResolverConfiguration.CONF.build();
    final Configuration implConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();

    return DriverLauncher.getLauncher(runtimeConfiguration)
        .run(Configurations.merge(driverConfiguration, cmdLineConf, metricServiceDriverConf,
        etMasterConfiguration, nameServerConfiguration, nameClientConfiguration,
        implConfiguration), JOB_TIMEOUT);
  }

  @NamedParameter(doc = "The period (in ms) to flush the metrics automatically.",
      default_value = "5000",
      short_name = "metric_automatic_flush_period_ms")
  final class MetricAutomaticFlushPeriodMs implements Name<Long> {
  }

  @NamedParameter(doc = "The period (in ms) to flush the metrics manually in the task.",
      default_value = "2000",
      short_name = "metric_manual_flush_period_ms")
  final class MetricManualFlushPeriodMs implements Name<Long> {
  }

  @NamedParameter(doc = "The period (in ms) to record the custom metrics of timestamp.",
      default_value = "100",
      short_name = "custom_metric_record_period_ms")
  final class CustomMetricRecordPeriodMs implements Name<Long> {
  }

  @NamedParameter(doc = "The duration (in ms) for Task to run.",
      default_value = "20000",
      short_name = "task_duration_ms")
  final class TaskDurationMs implements Name<Long> {
  }
}
