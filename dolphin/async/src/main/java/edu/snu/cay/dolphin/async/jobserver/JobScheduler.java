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

/**
 * A scheduler interface.
 * Different scheduling policy can be used with different implementations.
 */
public interface JobScheduler {

  /**
   * Invoke when a new job is submitted from client-side.
   * The job will be scheduled immediately or later, depending of the scheduler policy.
   * @param jobEntity a {@link JobEntity}
   */
  void onJobArrival(JobEntity jobEntity);

  /**
   * Invoke when a running job finishes, which means resources become available for other jobs.
   * @param numReleasedResources the number of released resources by job finish
   */
  void onJobFinish(int numReleasedResources);

  /**
   * Invoke when the amount of total resources changes,
   * which means waiting jobs can be started or running jobs should cancelled.
   * @param delta a delta of the number of resources
   */
  void onResourceChange(int delta);
}
