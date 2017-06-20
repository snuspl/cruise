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
 * A metadata of {@link JobConfiguration}.
 */
public final class JobConfigurationMetadata {

  private String jobId;
  private int maxNumEpochs;
  private int numServers;
  private int numWorkers;
  private String inputDir;
  private int miniBatchSize;
  private int numWorkerBlocks;

  public String getJobId() {
    return jobId;
  }

  public int getNumServers() {
    return numServers;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public String getInputDir() {
    return inputDir;
  }

  public int getMiniBatchSize() {
    return miniBatchSize;
  }

  public int getNumWorkerBlocks() {
    return numWorkerBlocks;
  }

  public int getMaxNumEpochs() {

    return maxNumEpochs;
  }
}
