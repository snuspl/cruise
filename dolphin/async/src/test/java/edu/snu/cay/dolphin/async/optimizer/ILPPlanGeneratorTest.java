/*
<<<<<<< HEAD
<<<<<<< HEAD
 * Copyright (C) 2017 Seoul National University
=======
 * Copyright (C) 2016 Seoul National University
>>>>>>> in progress
=======
 * Copyright (C) 2017 Seoul National University
>>>>>>> after add comments
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

import edu.snu.cay.dolphin.async.optimizer.impl.ILPPlanGenerator;
import edu.snu.cay.dolphin.async.optimizer.parameters.Constants;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;
import edu.snu.cay.dolphin.async.plan.impl.ILPPlanDescriptor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link ILPPlanGenerator}'s plan generation.
 */
public final class ILPPlanGeneratorTest {

  private ILPPlanGenerator ilpPlanGenerator;
  private int[] oldRole, newRole, oldDataBlockNum, newDataBlockNum, oldModelBlockNum, newModelBlockNum;
  
  @Before
  public void setup() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    ilpPlanGenerator = injector.getInstance(ILPPlanGenerator.class);

    oldRole = new int[] {0, 0, 0, 1, 1, 1};
    newRole = new int[] {0, 0, 1, 1, 1, 0};
    oldDataBlockNum = new int[] {83, 61, 45, 0, 0, 0};
    newDataBlockNum = new int[] {52, 47, 0, 0, 0, 90};
    oldModelBlockNum = new int[] {0, 0, 0, 32, 11, 42};
    newModelBlockNum = new int[] {0, 0, 27, 39, 19, 0};
  }

  @Test
  public void testILPPlanGeneration() {
    final ILPPlanDescriptor ilpPlanDescriptor =
        ilpPlanGenerator.generatePlanDescriptor(oldRole, oldDataBlockNum, oldModelBlockNum, newRole, newDataBlockNum,
            newModelBlockNum);
    final List<Integer> serverEvaluatorToAdd = ilpPlanDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_SERVER);
    final List<Integer> workerEvaluatorToAdd = ilpPlanDescriptor.getEvaluatorsToAdd(Constants.NAMESPACE_WORKER);
    final List<Integer> serverEvaluatorToDelete = ilpPlanDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_SERVER);
    final List<Integer> workerEvaluatorToDelete = ilpPlanDescriptor.getEvaluatorsToDelete(Constants.NAMESPACE_WORKER);
    System.out.println("Added servers");
    for (final Integer addedServer : serverEvaluatorToAdd) {
      System.out.print(addedServer + "  ");
    }
    System.out.println();

    System.out.println("Added workers");
    for (final Integer addedWorker : workerEvaluatorToAdd) {
      System.out.print(addedWorker + "  ");
    }
    System.out.println();

    System.out.println("Deleted servers");
    for (final Integer deletedServer : serverEvaluatorToDelete) {
      System.out.print(deletedServer + "  ");
    }
    System.out.println();

    System.out.println("Deleted workers");
    for (final Integer deletedWorkers : workerEvaluatorToDelete) {
      System.out.print(deletedWorkers + "  ");
    }
    System.out.println();

    System.out.println("Server transfer plan");
    for (final TransferStep transferStep : ilpPlanDescriptor.getTransferSteps(Constants.NAMESPACE_SERVER)) {
      System.out.println("From " + transferStep.getSrcId() + " to " + transferStep.getDstId() +
          " NumBlocks " + transferStep.getDataInfo().getNumBlocks());
    }

    System.out.println("Worker transfer plan");
    for (final TransferStep transferStep : ilpPlanDescriptor.getTransferSteps(Constants.NAMESPACE_WORKER)) {
      System.out.println("From " + transferStep.getSrcId() + " to " + transferStep.getDstId() +
          " NumBlocks " + transferStep.getDataInfo().getNumBlocks());
    }
  }
}
