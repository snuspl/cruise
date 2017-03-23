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
package edu.snu.cay.services.em.remote;

import edu.snu.cay.services.em.common.parameters.RangeSupport;
import edu.snu.cay.services.em.examples.remote.RemoteEMREEF;
import edu.snu.cay.utils.test.IntegrationTests;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

/**
 * Remote access test that uses a RemoteEMREEF example app.
 * It runs the RemoteEMREEF app and confirms that the app finishes with the correct status.
 * This test completely depends on the RemoteEMREEF example.
 * So it's better to modify the RemoteEMREEF example, when you wanna change the test.
 */
@Category(IntegrationTests.class)
public class RemoteAccessTest {

  private static final int TIMEOUT_MS = 100000;

  @Test
  @Category(IntegrationTests.class)
  public void runSingleKeyAccessTest() throws InjectionException {
    final boolean rangeSupport = false;
    final LauncherStatus status = runTest(rangeSupport);

    assertEquals(LauncherStatus.COMPLETED, status);
  }

  @Test
  @Category(IntegrationTests.class)
  public void runRangeKeyAccessTest() throws InjectionException {
    final boolean rangeSupport = true;
    final LauncherStatus status = runTest(rangeSupport);

    assertEquals(LauncherStatus.COMPLETED, status);
  }

  private LauncherStatus runTest(final boolean rangeSupport) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF.build();
    final HTraceParameters traceParameters = getTraceParameters(injector);
    final Configuration traceConf = traceParameters.getConfiguration();

    try {
      final Configuration jobConf = Configurations.merge(
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(RangeSupport.class, String.valueOf(rangeSupport))
              .build(),
          traceConf);
      return RemoteEMREEF.runRemoteEM(runtimeConf, jobConf, TIMEOUT_MS);
    } catch (final InjectionException e) {
      return LauncherStatus.failed(e);
    }
  }


  /**
   * Get HTraceParameters from the parsed command line Injector.
   */
  private static HTraceParameters getTraceParameters(final Injector injector) throws InjectionException {
    return injector.getInstance(HTraceParameters.class);
  }
}
