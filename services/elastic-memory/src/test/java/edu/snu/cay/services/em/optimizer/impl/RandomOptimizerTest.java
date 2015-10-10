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

import edu.snu.cay.services.em.optimizer.api.DataInfo;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.plan.api.TransferStep;
import edu.snu.cay.services.em.plan.api.Plan;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test that the Random Optimizer behaves correctly.
 */
public final class RandomOptimizerTest {

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
    final Collection<EvaluatorParameters> evaluators = getUniformEvaluators(dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators);

    assertEquals(0, plan.getEvaluatorsToAdd().size());
    assertEquals(0, plan.getEvaluatorsToDelete().size());
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
    final Collection<EvaluatorParameters> evaluators = getUniformEvaluators(dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators / 2);

    assertEquals(0, plan.getEvaluatorsToAdd().size());
    assertEquals(numEvaluators / 2, plan.getEvaluatorsToDelete().size());
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
    final Collection<EvaluatorParameters> evaluators = getUniformEvaluators(dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators * 2);

    assertEquals(numEvaluators, plan.getEvaluatorsToAdd().size());
    assertEquals(0, plan.getEvaluatorsToDelete().size());
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
    final Collection<EvaluatorParameters> evaluators = getUniformEvaluators(dataPerEvaluator, numEvaluators);

    final Plan plan = randomOptimizer.optimize(evaluators, numEvaluators * 3 / 2);

    assertNotNull(plan.getEvaluatorsToAdd());
    assertNotNull(plan.getEvaluatorsToDelete());
    assertNotNull(plan.getTransferSteps());
  }

  /**
   * Test that the optimizer runs correctly with multiple data types.
   * In particular,
   *   (sameNumEvaluatorsPlan) no evaluators are added or deleted when
   *     random optimizer is given a min, max of 1.0, 1.0 and
   *     the available evaluators is the same as the previous number of evaluators.
   *   (singleEvaluatorPlan) given a single evaluator is deleted, the
   *     expected data types and number of units are planned for transfer.
   *
   */
  @Test
  public void testMultipleDataTypes() {
    final RandomOptimizer randomOptimizer = getRandomOptimizer(1.0, 1.0);
    final List<EvaluatorParameters> evaluators = new ArrayList<>();

    final List<DataInfo> dataInfo1 = new ArrayList<>();
    dataInfo1.add(new DataInfoImpl("dataTypeB", 300));
    evaluators.add(new EvaluatorParametersImpl("1", dataInfo1, new HashMap<String, Double>(0)));

    final List<DataInfo> dataInfo2 = new ArrayList<>();
    dataInfo2.add(new DataInfoImpl("dataTypeA", 1000));
    dataInfo2.add(new DataInfoImpl("dataTypeB", 500));
    evaluators.add(new EvaluatorParametersImpl("2", dataInfo2, new HashMap<String, Double>(0)));

    final Plan sameNumEvaluatorsPlan = randomOptimizer.optimize(evaluators, evaluators.size());

    assertEquals(0, sameNumEvaluatorsPlan.getEvaluatorsToAdd().size());
    assertEquals(0, sameNumEvaluatorsPlan.getEvaluatorsToDelete().size());

    final Plan singleEvaluatorPlan = randomOptimizer.optimize(evaluators, 1);

    assertEquals(0, singleEvaluatorPlan.getEvaluatorsToAdd().size());
    assertEquals(1, singleEvaluatorPlan.getEvaluatorsToDelete().size());

    final String evaluatorToDelete = singleEvaluatorPlan.getEvaluatorsToDelete().iterator().next();
    assertEquals("2", evaluatorToDelete);
    assertEquals(2, singleEvaluatorPlan.getTransferSteps().size());

    long sum = 0;
    for (final TransferStep transferStep : singleEvaluatorPlan.getTransferSteps()) {
      assertEquals("2", transferStep.getSrcId());
      assertEquals("1", transferStep.getDstId());
      sum += transferStep.getDataInfo().getNumUnits();
    }
    assertEquals(1000 + 500, sum);
  }

  private Collection<EvaluatorParameters> getUniformEvaluators(final long dataPerEvaluator, final int numEvaluators) {
    final List<EvaluatorParameters> evaluators = new ArrayList<>(numEvaluators);
    for (int i = 0; i < numEvaluators; i++) {
      final List<DataInfo> dataInfos = new ArrayList<>(1);
      dataInfos.add(new DataInfoImpl("testType", (int) dataPerEvaluator));
      evaluators.add(new EvaluatorParametersImpl("test-" + i, dataInfos, new HashMap<String, Double>(0)));
    }
    return evaluators;
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
