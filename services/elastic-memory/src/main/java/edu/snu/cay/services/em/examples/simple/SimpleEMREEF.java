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

package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.common.aggregation.AggregationConfiguration;
import edu.snu.cay.services.em.common.parameters.EMTraceEnabled;
import edu.snu.cay.services.em.common.parameters.NumTotalBlocks;
import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.driver.EMConfProvider;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.RoundRobinDataIdFactory;
import edu.snu.cay.services.em.examples.simple.parameters.NumMoves;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client code for SimpleEM.
 */
public final class SimpleEMREEF {
  private static final Logger LOG = Logger.getLogger(SimpleEMREEF.class.getName());
  private static final int TIMEOUT = 100000;
  private static final Tang TANG = Tang.Factory.getTang();
  private static final int NUM_BLOCKS = 30; // As 3 evaluators will be allocated, each evaluator will own 10 blocks

  @NamedParameter(doc = "Whether or not to run on the local runtime", short_name = "local", default_value = "true")
  private static final class OnLocal implements Name<Boolean> {
  }

  @NamedParameter(doc = "Whether or not to support range in MemoryStore",
                  short_name = "range_support",
                  default_value = "false")
  private static final class RangeSupportName implements Name<Boolean> {
  }

  /**
   * Should not be instantiated.
   */
  private SimpleEMREEF() {
  }

  /**
   * Setup (register short names) and parse the command line, returning an Injector.
   */
  private static Injector parseCommandLine(final String[] args) throws InjectionException, IOException {
    final JavaConfigurationBuilder cb = TANG.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(cb);

    cl.registerShortNameOfClass(OnLocal.class);
    cl.registerShortNameOfClass(RangeSupportName.class);
    HTraceParameters.registerShortNames(cl);
    cl.registerShortNameOfClass(NumMoves.class);
    cl.registerShortNameOfClass(EMTraceEnabled.class);

    cl.processCommandLine(args);
    return TANG.newInjector(cb.build());
  }

  /**
   * Get onLocal from the parsed command line Injector.
   */
  private static boolean getOnLocal(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(OnLocal.class);
  }

  /**
   * Get RangeSupoprt from the parsed command line injector.
   */
  private static boolean getRangeSupport(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(RangeSupportName.class);
  }

  /**
   * Get HTraceParameters from the parsed command line Injector.
   */
  private static HTraceParameters getTraceParameters(final Injector injector) throws InjectionException {
    return injector.getInstance(HTraceParameters.class);
  }

  /**
   * Get NumMoves parameter from the parsed command line Injector.
   */
  private static int getNumMoves(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(NumMoves.class);
  }

  private static boolean getEMTraceEnabled(final Injector injector) throws InjectionException {
    return injector.getNamedInstance(EMTraceEnabled.class);
  }

  private static Configuration getDriverConfiguration() {
    final Configuration driverConfiguration = DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(SimpleEMDriver.class))
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "SimpleEMDriver")
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, SimpleEMDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STARTED, SimpleEMDriver.DriverStartHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, SimpleEMDriver.ActiveContextHandler.class)
        .build();

    final Configuration aggregationConf = AggregationConfiguration.newBuilder()
        .addAggregationClient(SimpleEMDriver.AGGREGATION_CLIENT_ID,
            DriverSideMsgHandler.class,
            EvalSideMsgHandler.class)
        .build()
        .getDriverConfiguration();

    // spawn the name server at the driver
    return Configurations.merge(driverConfiguration,
        EMConfProvider.getDriverConfiguration(),
        aggregationConf,
        NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        Tang.Factory.getTang().newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  static LauncherStatus runSimpleEM(final Configuration runtimeConf, final Configuration jobConf,
                                           final int timeOut)
      throws InjectionException {
    final Configuration driverConf = getDriverConfiguration();

    return DriverLauncher.getLauncher(runtimeConf).run(Configurations.merge(driverConf, jobConf), timeOut);
  }

  public static void main(final String[] args) throws InjectionException, IOException {
    final Injector injector = parseCommandLine(args);
    final boolean onLocal = getOnLocal(injector);
    final boolean rangeSupport = getRangeSupport(injector);
    final Configuration runtimeConf = onLocal ?
        LocalRuntimeConfiguration.CONF.build() :
        YarnClientConfiguration.CONF.build();

    final HTraceParameters traceParameters = getTraceParameters(injector);
    final Configuration traceConf = traceParameters.getConfiguration();

    final Configuration emConf = TANG.newConfigurationBuilder()
        .bindNamedParameter(RangeSupport.class, Boolean.toString(rangeSupport))
        .bindNamedParameter(NumTotalBlocks.class, Integer.toString(NUM_BLOCKS))
        .bindImplementation(DataIdFactory.class, RoundRobinDataIdFactory.class)
        .build();

    final Configuration exampleConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumMoves.class, Integer.toString(getNumMoves(injector)))
        .bindNamedParameter(EMTraceEnabled.class, Boolean.toString(getEMTraceEnabled(injector)))
        .build();

    final LauncherStatus status = runSimpleEM(runtimeConf,
        Configurations.merge(traceConf, emConf, exampleConf), TIMEOUT);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }
}
