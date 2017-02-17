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

import org.apache.reef.util.Builder;
import org.apache.reef.util.BuilderUtils;

/**
 * Encapsulates the information of a mini-batch.
 */
public final class MiniBatchInfo {
  private int epochIdx;
  private int miniBatchIdx;

  private MiniBatchInfo(final int epochIdx, final int miniBatchIdx) {
    this.epochIdx = epochIdx;
    this.miniBatchIdx = miniBatchIdx;
  }

  /**
   * @return A builder to create an MiniBatchInfo object.
   */
  public static MiniBatchInfoBuilder getBuilder() {
    return new MiniBatchInfoBuilder();
  }

  public int getEpochIdx() {
    return epochIdx;
  }

  public int getMiniBatchIdx() {
    return miniBatchIdx;
  }

  static final class MiniBatchInfoBuilder implements Builder<MiniBatchInfo> {
    private int epochIdx;
    private int miniBatchIdx;

    MiniBatchInfoBuilder setEpochIdx(final int epochIdx) {
      this.epochIdx = epochIdx;
      return this;
    }

    MiniBatchInfoBuilder setMiniBatchIdx(final int miniBatchIdx) {
      this.miniBatchIdx = miniBatchIdx;
      return this;
    }

    @Override
    public MiniBatchInfo build() {
      BuilderUtils.notNull(epochIdx);
      BuilderUtils.notNull(miniBatchIdx);
      return new MiniBatchInfo(epochIdx, miniBatchIdx);
    }
  }
}
