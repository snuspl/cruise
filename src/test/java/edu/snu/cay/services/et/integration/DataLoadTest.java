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

package edu.snu.cay.services.et.integration;

import edu.snu.cay.services.et.examples.load.LoadET;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Test class for running a load app.
 */
public class DataLoadTest {

  @Test
  public void testLoadET() throws InjectionException, IOException {
    assertEquals(LauncherStatus.COMPLETED,
        LoadET.runLoadET(new String[]{
            "-kv_data_path", "bin/inputs/key_list",
            "-none_key_data_path", "bin/inputs/none_key_list"}));
  }
}
