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
package edu.snu.cay.services.et.integration;

import edu.snu.cay.common.param.Parameters;
import edu.snu.cay.services.et.examples.addinteger.AddIntegerET;
import edu.snu.cay.services.et.examples.addinteger.parameters.*;
import edu.snu.cay.services.et.examples.simple.SimpleET;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for running example apps.
 */
public class ExampleTest {

  @Test
  public void testSimpleET() throws InjectionException {
    final String sampleInput = (ClassLoader.getSystemResource("data").getPath() + "/empty_file");
    assertEquals(LauncherStatus.COMPLETED, SimpleET.runSimpleET(sampleInput));
  }

  @Test
  public void testAddIntegerET() throws InjectionException {
    final Configuration commandLineConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NumWorkers.class, Integer.toString(2))
        .bindNamedParameter(NumServers.class, Integer.toString(2))
        .bindNamedParameter(NumUpdates.class, Integer.toString(128))
        .bindNamedParameter(StartKey.class, Integer.toString(0))
        .bindNamedParameter(NumKeys.class, Integer.toString(8))
        .bindNamedParameter(Parameters.Timeout.class, Integer.toString(30000))
        .build();

    assertEquals(LauncherStatus.COMPLETED, AddIntegerET.runAddIntegerET(commandLineConf));
  }
}
