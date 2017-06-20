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
package edu.snu.cay.services.evalmanager.impl;

import edu.snu.cay.utils.test.IntensiveTest;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests that {@link HomogeneousEvalManager} requests for evaluators and handles REEF events correctly.
 */
public final class HomogeneousEvalManagerTest {
  private final EvaluatorManagerTestHelper evaluatorManagerTestHelper = new EvaluatorManagerTestHelper();

  @Before
  public void setUp() throws InjectionException {
    evaluatorManagerTestHelper.setUp(false);
  }

  /**
   * Tests single plan with a single context submit.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanSingleContext() {
    evaluatorManagerTestHelper.testSinglePlanSingleContext();
  }

  /**
   * Tests single plan with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanMultipleContext() {
    evaluatorManagerTestHelper.testSinglePlanMultipleContext();
  }

  /**
   * Tests multiple plans with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  @Category(IntensiveTest.class)
  public void testMultiplePlanMultipleContext() {
    evaluatorManagerTestHelper.testMultiplePlanMultipleContext();
  }
}
