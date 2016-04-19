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

import edu.snu.cay.services.em.examples.remote.RemoteEMREEF;
import edu.snu.cay.utils.trace.HTraceParameters;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Remote access test that uses a RemoteEMREEF example app.
 * It runs the RemoteEMREEF app and confirms that the app finishes with the correct status.
 * This test completely depends on the RemoteEMREEF example.
 * So it's better to modify the RemoteEMREEF example, when you wanna change the test.
 */
public class RemoteAccessTest {

  private static final int TIMEOUT_MS = 100000;

  @Test
  public void runTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();

    final Configuration runtimeConf = LocalRuntimeConfiguration.CONF.build();
    final HTraceParameters traceParameters = getTraceParameters(injector);
    final Configuration traceConf = traceParameters.getConfiguration();

    LauncherStatus status;
    try {
      status = RemoteEMREEF.runRemoteEM(runtimeConf, traceConf, TIMEOUT_MS);
    } catch (final InjectionException e) {
      status = LauncherStatus.failed(e);
    }

    assertEquals(LauncherStatus.COMPLETED, status);
  }

  /**
   * Get HTraceParameters from the parsed command line Injector.
   */
  private static HTraceParameters getTraceParameters(final Injector injector) throws InjectionException {
    return injector.getInstance(HTraceParameters.class);
  }
}
