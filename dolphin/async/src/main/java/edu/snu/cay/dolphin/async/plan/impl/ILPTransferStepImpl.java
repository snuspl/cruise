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
package edu.snu.cay.dolphin.async.plan.impl;

import edu.snu.cay.dolphin.async.optimizer.api.DataInfo;
import edu.snu.cay.dolphin.async.plan.api.TransferStep;

/**
 * A plain-old-data implementation of TransferStep.
 */
public class ILPTransferStepImpl implements TransferStep {
  private final String srcId;
  private final String dstId;
  private final DataInfo dataInfo;

  public ILPTransferStepImpl(final String srcId, final String dstId, final DataInfo dataInfo) {
    this.srcId = srcId;
    this.dstId = dstId;
    this.dataInfo = dataInfo;
  }

  @Override
  public String getSrcId() {
    return srcId;
  }

  @Override
  public String getDstId() {
    return dstId;
  }

  @Override
  public DataInfo getDataInfo() {
    return dataInfo;
  }
}
