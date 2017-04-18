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

import edu.snu.cay.services.et.examples.tableaccess.TableAccessET;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test class for running a table access app.
 */
public class TableAccessTest {

  @Test
  public void testTableAccessET() throws InjectionException {
    assertEquals(LauncherStatus.COMPLETED, TableAccessET.runTableAccessET());
  }
}
