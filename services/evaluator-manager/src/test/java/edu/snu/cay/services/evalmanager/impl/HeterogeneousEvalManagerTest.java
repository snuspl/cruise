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
package edu.snu.cay.services.evalmanager.impl;

import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that {@link HeterogeneousEvalManager} requests for evaluators and handles REEF events correctly.
 */
public final class HeterogeneousEvalManagerTest {
  private final EvaluatorManagerTest evaluatorManagerTest = new EvaluatorManagerTest();

  @Before
  public void setUp() throws InjectionException {
    evaluatorManagerTest.setUp(true);
  }

  /**
   * Tests single plan with a single context submit.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanSingleContext() {
    evaluatorManagerTest.testSinglePlanSingleContext();
  }

  /**
   * Tests single plan with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testSinglePlanMultipleContext() {
    evaluatorManagerTest.testSinglePlanMultipleContext();
  }

  /**
   * Tests multiple plans with multiple context submits.
   * Checks that context stack is built correctly.
   */
  @Test
  public void testMultiplePlanMultipleContext() {
    evaluatorManagerTest.testMultiplePlanMultipleContext();
  }

  /**
   * Tests multiple requests for heterogeneous evaluators.
   * Checks that allocated evaluators's resource type is as requested.
   */
  @Test
  public void testHeteroEvalRequest() {
    evaluatorManagerTest.testHeteroEvalRequest();
  }
}
