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
package edu.snu.cay.dolphin.async.optimizer;

import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.services.em.optimizer.api.EvaluatorParameters;
import edu.snu.cay.services.em.optimizer.api.Optimizer;
import edu.snu.cay.dolphin.async.optimizer.SampleOptimizers.*;
import edu.snu.cay.services.em.optimizer.impl.DataInfoImpl;
import edu.snu.cay.services.em.optimizer.impl.EvaluatorParametersImpl;
import edu.snu.cay.services.em.plan.api.Plan;
import edu.snu.cay.services.em.plan.api.TransferStep;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link SampleOptimizers}'s plan generation according to the purpose of each optimizer.
 */
public final class SampleOptimizersTest {

  private static final String EVAL_PREFIX = "EVAL-";
  private List<EvaluatorParameters> evalParamsList;

  private Optimizer getOptimizer(final Class<? extends Optimizer> optimizerClass) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    return injector.getInstance(optimizerClass);
  }

  @Before
  public void setup() {
    evalParamsList = new ArrayList<>(5);
    evalParamsList.add(new EvaluatorParametersImpl(EVAL_PREFIX + 0, new DataInfoImpl(5), Collections.emptyMap()));
    evalParamsList.add(new EvaluatorParametersImpl(EVAL_PREFIX + 1, new DataInfoImpl(10), Collections.emptyMap()));
    evalParamsList.add(new EvaluatorParametersImpl(EVAL_PREFIX + 2, new DataInfoImpl(15), Collections.emptyMap()));
    evalParamsList.add(new EvaluatorParametersImpl(EVAL_PREFIX + 3, new DataInfoImpl(7), Collections.emptyMap()));
    evalParamsList.add(new EvaluatorParametersImpl(EVAL_PREFIX + 4, new DataInfoImpl(1), Collections.emptyMap()));
  }

  private EvaluatorParameters findEvalWithMostBlocks(final List<EvaluatorParameters> paramsList) {
    paramsList.sort((o1, o2) -> o2.getDataInfo().getNumBlocks() - o1.getDataInfo().getNumBlocks());
    return paramsList.get(0);
  }

  private EvaluatorParameters findEvalWithLeastBlocks(final List<EvaluatorParameters> paramsList) {
    paramsList.sort((o1, o2) -> o1.getDataInfo().getNumBlocks() - o2.getDataInfo().getNumBlocks());
    return paramsList.get(0);
  }

  private void testAddOneOptimizer(final Optimizer addOneOptimizer, final String namespace) {
    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>();
    evalParamsMap.put(namespace, evalParamsList);

    final Plan plan = addOneOptimizer.optimize(evalParamsMap, evalParamsList.size() + 1, Collections.emptyMap());

    // only one add plan
    final Collection<String> evalsToAdd = plan.getEvaluatorsToAdd(namespace);
    assertEquals(1, evalsToAdd.size());

    final String evalToAdd = evalsToAdd.iterator().next();

    // no delete plan
    assertTrue(plan.getEvaluatorsToDelete(namespace).isEmpty());

    // only one move plan
    final Collection<TransferStep> transferSteps = plan.getTransferSteps(namespace);
    assertEquals(1, transferSteps.size());

    // move half blocks from evaluator with most blocks
    final TransferStep transferStep = transferSteps.iterator().next();
    final EvaluatorParameters srcEval = findEvalWithMostBlocks(evalParamsList);
    assertEquals(srcEval.getDataInfo().getNumBlocks() / 2, transferStep.getDataInfo().getNumBlocks());
    assertEquals(srcEval.getId(), transferStep.getSrcId());
    assertEquals(evalToAdd, transferStep.getDstId());
  }

  /**
   * Tests whether {@link AddOneServerOptimizer} generates correct plan with given eval parameters.
   */
  @Test
  public void testAddOneServerOptimizer() throws InjectionException {
    final Optimizer addOneServerOptimizer = getOptimizer(AddOneServerOptimizer.class);

    testAddOneOptimizer(addOneServerOptimizer, Constants.NAMESPACE_SERVER);
  }

  /**
   * Tests whether {@link AddOneWorkerOptimizer} generates a correct plan with given eval parameters.
   */
  @Test
  public void testAddOneWorkerOptimizer() throws InjectionException {
    final Optimizer addOneWorkerOptimizer = getOptimizer(AddOneWorkerOptimizer.class);

    testAddOneOptimizer(addOneWorkerOptimizer, Constants.NAMESPACE_WORKER);
  }

  private void testDeleteOneOptimizer(final Optimizer deleteOneOptimizer, final String namespace) {
    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>();
    evalParamsMap.put(namespace, evalParamsList);

    final Plan plan = deleteOneOptimizer.optimize(evalParamsMap, evalParamsList.size(), Collections.emptyMap());

    // no add plan
    assertTrue(plan.getEvaluatorsToAdd(namespace).isEmpty());

    // only one delete plan
    final Collection<String> evalsToDel = plan.getEvaluatorsToDelete(namespace);
    assertEquals(1, evalsToDel.size());

    final String evalToDel = evalsToDel.iterator().next();

    // only one move plan
    final Collection<TransferStep> transferSteps = plan.getTransferSteps(namespace);
    assertEquals(1, transferSteps.size());

    // delete evaluator with least block and move blocks to evaluator with secondly least blocks
    final TransferStep transferStep = transferSteps.iterator().next();
    final EvaluatorParameters deleteEvalParams = findEvalWithLeastBlocks(evalParamsList);
    assertEquals(deleteEvalParams.getDataInfo().getNumBlocks(), transferStep.getDataInfo().getNumBlocks());
    assertEquals(evalToDel, transferStep.getSrcId());

    evalParamsList.remove(deleteEvalParams);
    final EvaluatorParameters dstEvalParams = findEvalWithLeastBlocks(evalParamsList);
    assertEquals(dstEvalParams.getId(), transferStep.getDstId());
  }

  /**
   * Tests whether {@link DeleteOneWorkerOptimizer} generates a correct plan with given eval parameters.
   */
  @Test
  public void testDeleteOneServerOptimizer() throws InjectionException {
    final Optimizer deleteOneServerOptimizer = getOptimizer(DeleteOneServerOptimizer.class);

    testDeleteOneOptimizer(deleteOneServerOptimizer, Constants.NAMESPACE_SERVER);
  }

  /**
   * Tests whether {@link DeleteOneWorkerOptimizer} generates a correct plan with given eval parameters.
   */
  @Test
  public void testDeleteOneWorkerOptimizer() throws InjectionException {
    final Optimizer deleteOneWorkerOptimizer = getOptimizer(DeleteOneWorkerOptimizer.class);

    testDeleteOneOptimizer(deleteOneWorkerOptimizer, Constants.NAMESPACE_WORKER);
  }

  /**
   * Tests whether {@link ExchangeOneOptimizer} generates a correct plan with given eval parameters.
   * Since it randomly choose the evaluators we cannot verify the plan in fine-grained manner.
   */
  @Test
  public void testExchangeOneOptimizer() throws InjectionException {
    final Optimizer exchangeOneOptimizer = getOptimizer(ExchangeOneOptimizer.class);

    final Map<String, List<EvaluatorParameters>> evalParamsMap = new HashMap<>();
    evalParamsMap.put(Constants.NAMESPACE_SERVER, evalParamsList);
    evalParamsMap.put(Constants.NAMESPACE_WORKER, evalParamsList);

    final Plan plan = exchangeOneOptimizer.optimize(evalParamsMap, evalParamsList.size() * 2, Collections.emptyMap());

    final Collection<String> serverEvalsToAdd = plan.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER);
    final Collection<String> serverEvalsToDel = plan.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER);
    final Collection<TransferStep> serverTransferSteps = plan.getTransferSteps(Constants.NAMESPACE_SERVER);

    final Collection<String> workerEvalsToAdd = plan.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER);
    final Collection<String> workerEvalsToDel = plan.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER);
    final Collection<TransferStep> workerTransferSteps = plan.getTransferSteps(Constants.NAMESPACE_WORKER);

    // only one add and del in each namespace
    assertEquals(1, serverEvalsToAdd.size() + workerEvalsToAdd.size());
    assertEquals(1, serverEvalsToDel.size() + workerEvalsToDel.size());

    // namespace cannot have both add and del together
    assertEquals(1, serverEvalsToAdd.size() + serverEvalsToDel.size());
    assertEquals(1, workerEvalsToAdd.size() + workerEvalsToDel.size());

    // this optimizer should generate move plans, because in the given eval params there's no empty evals
    assertEquals(1, serverTransferSteps.size());
    assertEquals(1, workerTransferSteps.size());
  }
}
