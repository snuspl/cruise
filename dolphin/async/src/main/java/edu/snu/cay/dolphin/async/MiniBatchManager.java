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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by xyzi on 09/07/2017.
 */
@DriverSide
public final class MiniBatchManager {
  private static final Logger LOG = Logger.getLogger(MiniBatchManager.class.getName());

  private final int numTotalMiniBatchesInEpoch;

  private final Map<Integer, AtomicInteger> epochIdxTobatchCountMap = new ConcurrentHashMap<>();

  private final MasterSideMsgSender msgSender;

  @Inject
  private MiniBatchManager(final MasterSideMsgSender msgSender,
                           @Parameter(DolphinParameters.NumTotalMiniBatches.class) final int numTotalMiniBatches) {
    this.numTotalMiniBatchesInEpoch = numTotalMiniBatches;
    this.msgSender = msgSender;
  }

  public void onBatchReq(final String workerId, final BatchReqMsg batchReqMsg) {
    final int epochIdx = batchReqMsg.getEpochIdx();
    final AtomicInteger batchCounter = epochIdxTobatchCountMap.computeIfAbsent(epochIdx, x -> new AtomicInteger());
    final int currBatchCnt = batchCounter.getAndIncrement();
    final boolean allow = currBatchCnt < numTotalMiniBatchesInEpoch;
    if (allow) {
      LOG.log(Level.INFO, "{0} th batch in epoch {1} allowed to worker {2}",
          new Object[]{currBatchCnt, epochIdx, workerId});
    } else {
      LOG.log(Level.INFO, "Worker {0} is not allowed to run more mini-batch for epoch {1}",
          new Object[]{workerId, epochIdx});
    }
    msgSender.sendBatchResMsg(workerId, allow);
  }
}
