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
package edu.snu.spl.cruise.ps.jobserver.driver;

import edu.snu.spl.cruise.services.et.configuration.ExecutorConfiguration;
import edu.snu.spl.cruise.services.et.configuration.TableConfiguration;
import org.apache.reef.tang.Injector;

/**
 * A class for encapsulating a job waiting to be scheduled.
 */
public final class JobEntity {
  private final Injector jobInjector;
  private final String jobId;

  private final int numServers;
  private final ExecutorConfiguration serverExecutorConf;
  private final TableConfiguration serverTableConf;

  private final int numWorkers;
  private final ExecutorConfiguration workerExecutorConf;
  private final TableConfiguration workerTableConf;
  private final String inputPath;

  private JobEntity(final Injector jobInjector,
                   final String jobId,
                   final int numServers,
                   final ExecutorConfiguration serverExecutorConf,
                   final TableConfiguration serverTableConf,
                   final int numWorkers,
                   final ExecutorConfiguration workerExecutorConf,
                   final TableConfiguration workerTableConf,
                   final String inputPath) {
    this.jobInjector = jobInjector;
    this.jobId = jobId;
    this.numServers = numServers;
    this.serverExecutorConf = serverExecutorConf;
    this.serverTableConf = serverTableConf;
    this.numWorkers = numWorkers;
    this.workerExecutorConf = workerExecutorConf;
    this.workerTableConf = workerTableConf;
    this.inputPath = inputPath;
  }

  public Injector getJobInjector() {
    return jobInjector;
  }

  public String getJobId() {
    return jobId;
  }

  public int getNumServers() {
    return numServers;
  }

  public ExecutorConfiguration getServerExecutorConf() {
    return serverExecutorConf;
  }

  public TableConfiguration getServerTableConf() {
    return serverTableConf;
  }

  public int getNumWorkers() {
    return numWorkers;
  }

  public ExecutorConfiguration getWorkerExecutorConf() {
    return workerExecutorConf;
  }

  public TableConfiguration getWorkerTableConf() {
    return workerTableConf;
  }

  public String getInputPath() {
    return inputPath;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<JobEntity> {

    private Injector jobInjector;
    private String jobId;

    private int numServers;
    private ExecutorConfiguration serverExecutorConf;
    private TableConfiguration serverTableConf;

    private int numWorkers;
    private ExecutorConfiguration workerExecutorConf;
    private TableConfiguration workerTableConf;
    private String inputPath;

    private Builder() {
    }

    public Builder setJobInjector(final Injector jobInjector) {
      this.jobInjector = jobInjector;
      return this;
    }

    public Builder setJobId(final String jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setNumServers(final int numServers) {
      this.numServers = numServers;
      return this;
    }

    public Builder setServerExecutorConf(final ExecutorConfiguration serverExecutorConf) {
      this.serverExecutorConf = serverExecutorConf;
      return this;
    }

    public Builder setServerTableConf(final TableConfiguration serverTableConf) {
      this.serverTableConf = serverTableConf;
      return this;
    }

    public Builder setNumWorkers(final int numWorkers) {
      this.numWorkers = numWorkers;
      return this;
    }

    public Builder setWorkerExecutorConf(final ExecutorConfiguration workerExecutorConf) {
      this.workerExecutorConf = workerExecutorConf;
      return this;
    }

    public Builder setWorkerTableConf(final TableConfiguration workerTableConf) {
      this.workerTableConf = workerTableConf;
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      this.inputPath = inputPath;
      return this;
    }

    @Override
    public JobEntity build() {
      return new JobEntity(jobInjector, jobId, numServers, serverExecutorConf, serverTableConf,
          numWorkers, workerExecutorConf, workerTableConf, inputPath);
    }
  }
}
