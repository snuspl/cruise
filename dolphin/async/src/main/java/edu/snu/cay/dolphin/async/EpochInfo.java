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
package edu.snu.cay.dolphin.async;

import org.apache.reef.util.BuilderUtils;

/**
 * Encapsulates the information of an epoch.
 */
public final class EpochInfo {
  private final int epochIdx;
  private final int numMiniBatches;
  private final int numEMBlocks;
  private final long epochStartTime;

  private EpochInfo(final int epochIdx, final int numMiniBatches, final int numEMBlocks, final long epochStartTime) {
    this.epochIdx = epochIdx;
    this.numMiniBatches = numMiniBatches;
    this.numEMBlocks = numEMBlocks;
    this.epochStartTime = epochStartTime;
  }

  /**
   * @return A builder to create an EpochInfo object
   */
  public static EpochInfoBuilder getBuilder() {
    return new EpochInfoBuilder();
  }

  /**
   * @return the index of the epoch
   */
  public int getEpochIdx() {
    return epochIdx;
  }

  /**
   * @return the number of mini-batches performed at this epoch
   */
  public int getNumMiniBatches() {
    return numMiniBatches;
  }

  /**
   * @return the number of EM blocks in the evaluator at this epoch
   */
  public int getNumEMBlocks() {
    return numEMBlocks;
  }

  /**
   * @return the time when this epoch started
   */
  public long getEpochStartTime() {
    return epochStartTime;
  }

  static final class EpochInfoBuilder implements org.apache.reef.util.Builder<EpochInfo> {
    private int epochIdx;
    private int numMiniBatches;
    private int numEMBlocks;
    private long epochStartTime;

    EpochInfoBuilder setEpochIdx(final int epochIdx) {
      this.epochIdx = epochIdx;
      return this;
    }

    EpochInfoBuilder setNumMiniBatches(final int numMiniBatches) {
      this.numMiniBatches = numMiniBatches;
      return this;
    }

    EpochInfoBuilder setNumEMBlocks(final int numEMBlocks) {
      this.numEMBlocks = numEMBlocks;
      return this;
    }

    EpochInfoBuilder setEpochStartTime(final long epochStartTime) {
      this.epochStartTime = epochStartTime;
      return this;
    }

    @Override
    public EpochInfo build() {
      BuilderUtils.notNull(epochIdx);
      BuilderUtils.notNull(numMiniBatches);
      BuilderUtils.notNull(numEMBlocks);
      BuilderUtils.notNull(epochStartTime);

      return new EpochInfo(epochIdx, numMiniBatches, numEMBlocks, epochStartTime);
    }
  }
}
