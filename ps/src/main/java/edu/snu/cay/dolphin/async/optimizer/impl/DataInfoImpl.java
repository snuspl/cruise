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
package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.dolphin.async.optimizer.api.DataInfo;

/**
 * A plain-old-data implementation of DataInfo.
 */
public final class DataInfoImpl implements DataInfo {
  private int numBlocks;

  /**
   * Creates a DataInfo.
   */
  public DataInfoImpl(final int numBlocks) {
    this.numBlocks = numBlocks;
  }

  /**
   * Creates an empty DataInfo that has no data.
   */
  public DataInfoImpl() {
    this(0);
  }

  @Override
  public int getNumBlocks() {
    return numBlocks;
  }

  @Override
  public void setNumBlocks(final int numBlocks) {
    this.numBlocks = numBlocks;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DataInfoImpl{");
    sb.append("numBlocks=").append(numBlocks);
    sb.append('}');
    return sb.toString();
  }
}
