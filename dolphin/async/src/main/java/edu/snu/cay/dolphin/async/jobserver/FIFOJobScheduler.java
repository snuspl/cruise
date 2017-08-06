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
package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A basic implementation of job scheduler based on FIFO policy.
 * It submits jobs in order, whenever resources are available.
 */
public final class FIFOJobScheduler implements JobScheduler {
  private static final Logger LOG = Logger.getLogger(FIFOJobScheduler.class.getName());

  private final Queue<JobEntity> jobWaitingQueue = new ConcurrentLinkedQueue<>();

  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;

  private int numAvailableResources;

  @Inject
  private FIFOJobScheduler(@Parameter(Parameters.NumTotalResources.class) final int numTotalResources,
                           final InjectionFuture<JobServerDriver> jobServerDriverFuture) {
    this.numAvailableResources = numTotalResources;
    this.jobServerDriverFuture = jobServerDriverFuture;
  }

  /**
   * Execute a new job immediately, if there're enough free resources.
   * Otherwise, put it into a queue so it can be executed when resources become available.
   */
  @Override
  public synchronized void onJobArrival(final JobEntity jobEntity) {
    final int numResourcesToUse = jobEntity.getNumWorkers() + jobEntity.getNumServers();
    if (numAvailableResources >= numResourcesToUse) {
      LOG.log(Level.INFO, "Start job {0} with {1} resources. Remaining free resources: {2}",
          new Object[]{jobEntity.getJobId(), numAvailableResources, numAvailableResources - numResourcesToUse});

      numAvailableResources -= numResourcesToUse;
      jobServerDriverFuture.get().executeJob(jobEntity);

    } else {
      LOG.log(Level.INFO, "Put job {0} into queue", jobEntity.getJobId());
      jobWaitingQueue.add(jobEntity);
    }
  }

  /**
   * Executes waiting jobs if the enough amount of resources become available for them.
   */
  @Override
  public synchronized void onJobFinish(final int numReleasedResources) {
    numAvailableResources += numReleasedResources;

    // start waiting jobs, if enough resources become available
    while (!jobWaitingQueue.isEmpty()) {
      final JobEntity jobEntity = jobWaitingQueue.peek();
      final int numResourcesToUse = jobEntity.getNumWorkers() + jobEntity.getNumServers();

      if (numAvailableResources >= numResourcesToUse) {
        LOG.log(Level.INFO, "Start job {0} with {1} resources. Remaining free resources: {2}",
            new Object[]{jobEntity.getJobId(), numAvailableResources, numAvailableResources - numResourcesToUse});

        numAvailableResources -= numResourcesToUse;
        jobServerDriverFuture.get().executeJob(jobEntity);

        jobWaitingQueue.poll();
      } else {
        break; // FIFO.
      }
    }
  }

  @Override
  public synchronized void onResourceChange(final int delta) {
    throw new UnsupportedOperationException("Resource availability is not supported for now");
  }
}
