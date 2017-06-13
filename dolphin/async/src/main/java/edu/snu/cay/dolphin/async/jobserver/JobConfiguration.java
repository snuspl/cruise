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
 * Job configuration of a Job Server on ET application.
 */
public final class JobConfiguration {

  private final String jobId;
  private final int numServer;
  private final int numWorker;

  private JobConfiguration(final JobConfigurationMetadata jobConfigurationMetadata) {
    this.jobId = jobConfigurationMetadata.getJobId();
    this.numServer = jobConfigurationMetadata.getNumServer();
    this.numWorker = jobConfigurationMetadata.getNumWorker();
  }

  public String getJobId() {
    return jobId;
  }

  public int getNumServer() {
    return numServer;
  }

  public int getNumWorker() {
    return numWorker;
  }

  public static JobConfiguration from(final JobConfigurationMetadata jobConfigurationMetadata) {
    return new JobConfiguration(jobConfigurationMetadata);
  }
}
