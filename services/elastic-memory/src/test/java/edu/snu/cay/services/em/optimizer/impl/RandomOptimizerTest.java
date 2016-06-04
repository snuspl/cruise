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
package edu.snu.cay.services.em.optimizer.impl;

import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.plan.api.Plan;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that the Random Optimizer behaves correctly.
 */
public final class RandomOptimizerTest {
  private static final String NAMESPACE = "OPTIMIZER";

  /**
   * Test that no evaluators are added or deleted when
   * random optimizer is given a min, max of 1.0, 1.0 and
   * the available evaluators is the same as the previous number of evaluators.
   */
  @Test
  public void testSameNumEvaluators() {
    final RandomOptimizer randomOptimizer = getRandomOptimizer(1.0, 1.0);
    final long dataPerEvaluator = 5000;
    final int numEvaluators = 10;
    final Map<String, List<EvaluatorParameters>> evaluators =
        getUniformEvaluators(NAMESPACE, dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators);

    assertEquals(0, plan.getEvaluatorsToAdd(NAMESPACE).size());
    assertEquals(0, plan.getEvaluatorsToDelete(NAMESPACE).size());
  }

  /**
   * Test that half the evaluators are deleted when
   * random optimizer is given a min, max of 1.0, 1.0 and
   * the available evaluators is half the previous number of evaluators.
   */
  @Test
  public void testHalfNumEvaluators() {
    final RandomOptimizer randomOptimizer = getRandomOptimizer(1.0, 1.0);
    final long dataPerEvaluator = 5000;
    final int numEvaluators = 10;
    final Map<String, List<EvaluatorParameters>> evaluators =
        getUniformEvaluators(NAMESPACE, dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators / 2);

    assertEquals(0, plan.getEvaluatorsToAdd(NAMESPACE).size());
    assertEquals(numEvaluators / 2, plan.getEvaluatorsToDelete(NAMESPACE).size());
  }

  /**
   * Test that evaluators are added to double the number of evaluators when
   * random optimizer is given a min, max of 1.0, 1.0 and
   * the available evaluators is twice the previous number of evaluators.
   */
  @Test
  public void testDoubleNumEvaluators() {
    final RandomOptimizer randomOptimizer = getRandomOptimizer(1.0, 1.0);
    final long dataPerEvaluator = 5000;
    final int numEvaluators = 10;
    final Map<String, List<EvaluatorParameters>> evaluators =
        getUniformEvaluators(NAMESPACE, dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators * 2);

    assertEquals(numEvaluators, plan.getEvaluatorsToAdd(NAMESPACE).size());
    assertEquals(0, plan.getEvaluatorsToDelete(NAMESPACE).size());
  }

  /**
   * Test that optimization runs without exception when
   * random optimizer is given a min, max of 0.5, 1.0 and
   * the available evaluators is 1.5 times the previous number of evaluators.
   */
  @Test
  public void testRandomNumEvaluators() {
    final RandomOptimizer randomOptimizer = getRandomOptimizer(0.5, 1.0);
    final long dataPerEvaluator = 5000;
    final int numEvaluators = 10;
    final Map<String, List<EvaluatorParameters>> evaluators =
        getUniformEvaluators(NAMESPACE, dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators * 3 / 2);

    assertNotNull(plan.getEvaluatorsToAdd(NAMESPACE));
    assertNotNull(plan.getEvaluatorsToDelete(NAMESPACE));
    assertNotNull(plan.getTransferSteps(NAMESPACE));
  }

  private Map<String, List<EvaluatorParameters>> getUniformEvaluators(final String namespace,
                                                                      final long dataPerEvaluator,
                                                                      final int numEvaluators) {
    final List<EvaluatorParameters> evalParamsList = new ArrayList<>(numEvaluators);
    for (int i = 0; i < numEvaluators; i++) {
      evalParamsList.add(
          new EvaluatorParametersImpl("test-" + i, new DataInfoImpl((int) dataPerEvaluator), new HashMap<>(0)));
    }

    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>(1);
    evalParamsMap.put(namespace, evalParamsList);
    return evalParamsMap;
  }

  private static RandomOptimizer getRandomOptimizer(final double minEvaluatorsFraction,
                                                    final double maxEvaluatorsFraction) {
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(RandomOptimizer.MinEvaluatorsFraction.class, Double.toString(minEvaluatorsFraction))
        .bindNamedParameter(RandomOptimizer.MaxEvaluatorsFraction.class, Double.toString(maxEvaluatorsFraction))
        .build();
    try {
      return Tang.Factory.getTang().newInjector(configuration).getInstance(RandomOptimizer.class);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
