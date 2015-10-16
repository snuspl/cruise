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
package edu.snu.cay.dolphin.integration.em;

import edu.snu.cay.dolphin.core.DolphinConfiguration;
import edu.snu.cay.dolphin.core.DolphinLauncher;
import edu.snu.cay.dolphin.core.UserJobInfo;
import edu.snu.cay.dolphin.examples.simple.SimpleJobInfo;
import edu.snu.cay.dolphin.parameters.JobIdentifier;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.parameters.DriverStartHandler;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * EM add integration test uses multi-thread to call EM add primitive.
 * Checks REEF job status and asserts whether it is completed without exception or not.
 * To implement a useful integration test, we should use DolphinDriver without changes
 * since it is tightly related to EM add primitive.
 * Thus this test uses DolphinDriver without any modification.
 * You can use any integer value for parameters of {@code run},
 * but be careful to not choose too large number to ensure that it finishes within a certain timeout.
 */
public class AddIntegrationTest {

  @Test
  public void runTest() {
    run(7, 4);
  }

  private void run(final int addEvalNum, final int addThreadNum) {
    final String[] args = {
        "-split", "1",
        "-local", "true",
        "-input", ClassLoader.getSystemResource("data").getPath() + "/sample",
        "-output", ClassLoader.getSystemResource("data").getPath() + "/result",
        "-maxNumEvalLocal", Integer.toString(addEvalNum + 2),
        "-timeout", "120000"
    };
    LauncherStatus status;
    try {
      status = DolphinLauncher.run(
          Configurations.merge(
              DolphinConfiguration.getConfiguration(args),
              Tang.Factory.getTang().newConfigurationBuilder()
                  .bindNamedParameter(JobIdentifier.class, "EM Add Integration Test")
                  .bindImplementation(UserJobInfo.class, SimpleJobInfo.class)
                  .build()),
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindSetEntry(DriverStartHandler.class, AddTestStartHandler.class)
              .bindNamedParameter(AddEvalNumber.class, Integer.toString(addEvalNum))
              .bindNamedParameter(AddThreadNumber.class, Integer.toString(addThreadNum))
              .build());
    } catch (IOException e) {
      status = LauncherStatus.failed(e);
    }
    assertEquals(LauncherStatus.COMPLETED, status);
  }

  @NamedParameter(doc = "Evaluator number for EM add primitive", short_name = "add_eval_number")
  public final class AddEvalNumber implements Name<Integer> {
  }

  @NamedParameter(doc = "Thread number for EM add primitive", short_name = "add_thread_number")
  public final class AddThreadNumber implements Name<Integer> {
  }
}
