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
package edu.snu.spl.cruise.services.et.configuration;

import org.apache.reef.util.BuilderUtils;

/**
 * Resource configuration of executor.
 */
public final class ResourceConfiguration {
  private final int numCores;
  private final int memSizeInMB;
  private final String[] nodeNames;

  private ResourceConfiguration(final int numCores, final int memSizeInMB, final String[] nodeNames) {
    this.numCores = numCores;
    this.memSizeInMB = memSizeInMB;
    this.nodeNames = nodeNames;
  }

  /**
   * @return the number of cores for each executors to use
   */
  public int getNumCores() {
    return numCores;
  }

  /**
   * @return the size of memory for each executors to use
   */
  public int getMemSizeInMB() {
    return memSizeInMB;
  }

  /**
   * @return an array of node names to allocate an executor on
   */
  public String[] getNodeNames() {
    return nodeNames;
  }

  /**
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of ResourceConfiguration.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ResourceConfiguration> {
    private Integer numCores;
    private Integer memSizeInMB;
    private String[] nodeNames = new String[0];

    private Builder() {
    }

    public Builder setNumCores(final int numCores) {
      this.numCores = numCores;
      return this;
    }

    public Builder setMemSizeInMB(final int memSizeInMB) {
      this.memSizeInMB = memSizeInMB;
      return this;
    }

    public Builder setNodeNames(final String[] nodeNames) {
      this.nodeNames = nodeNames;
      return this;
    }

    @Override
    public ResourceConfiguration build() {
      BuilderUtils.notNull(numCores);
      BuilderUtils.notNull(memSizeInMB);
      BuilderUtils.notNull(nodeNames);

      return new ResourceConfiguration(numCores, memSizeInMB, nodeNames);
    }
  }
}
