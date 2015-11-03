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
package edu.snu.cay.dolphin.examples.sleep;

import edu.snu.cay.dolphin.core.UserComputeTask;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceSender;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.evaluator.api.PartitionTracker;
import edu.snu.cay.services.em.exceptions.IdGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link UserComputeTask} for SleepREEF.
 * Retrieves the number of data units it is currently holding, from {@link MemoryStore},
 * multiplies that value with its computation speed,
 * and then sleeps for that amount of time using {@link Thread#sleep(long)}.
 */
public final class SleepCmpTask extends UserComputeTask implements DataReduceSender<Integer> {

  private final int initialWorkload;
  private final float computationSpeed;
  private final MemoryStore memoryStore;
  private final PartitionTracker partitionTracker;
  private final DataIdFactory<Long> dataIdFactory;

  @Inject
  private SleepCmpTask(@Parameter(SleepParameters.InitialWorkload.class) final int initialWorkload,
                       @Parameter(SleepParameters.ComputationSpeed.class) final float computationSpeed,
                       final MemoryStore memoryStore,
                       final PartitionTracker partitionTracker,
                       final DataIdFactory<Long> dataIdFactory) {
    this.initialWorkload = initialWorkload;
    this.computationSpeed = computationSpeed;
    this.memoryStore = memoryStore;
    this.partitionTracker = partitionTracker;
    this.dataIdFactory = dataIdFactory;
  }

  @Override
  public void initialize() {
    if (initialWorkload == 0) {
      return;
    }

    try {
      // insert the initial workload assigned to this task
      final List<Long> ids = dataIdFactory.getIds(initialWorkload);
      final List<Object> objects = new ArrayList<>(initialWorkload);

      // the actual data objects are not important; only the number of units is relevant
      // thus we use the same object for all ids
      final Object object = new Object();
      for (int index = 0; index < initialWorkload; ++index) {
        objects.add(object);
      }

      partitionTracker.registerPartition(SleepParameters.KEY, ids.get(0), ids.get(ids.size() - 1));
      memoryStore.getElasticStore().putList(SleepParameters.KEY, ids, objects);

    } catch (final IdGenerationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void run(final int iteration) {
    final int workload = memoryStore.getElasticStore().getNumUnits(SleepParameters.KEY);
    final long sleepTime = (long)(workload * computationSpeed);

    try {
      Thread.sleep(sleepTime);
    } catch (final InterruptedException e) {
      throw new RuntimeException("InterruptedException during sleeping", e);
    }
  }

  @Override
  public Integer sendReduceData(final int iteration) {
    return 1;
  }
}
