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

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xyzi on 09/07/2017.
 */
@EvaluatorSide
public final class MiniBatchController {

  private volatile CountDownLatch latch;
  private volatile boolean batchAllowed;

  private final WorkerSideMsgSender msgSender;

  @Inject
  private MiniBatchController(final WorkerSideMsgSender msgSender) {
    this.msgSender = msgSender;
  }

  public boolean inquireBatch(final int epochIdx, final int bathIdx) {
    latch = new CountDownLatch(1);
    batchAllowed = false;

    try {
      msgSender.sendBatchReqMsg(epochIdx);
    } catch (NetworkException e) {
      throw new RuntimeException(e);
    }

    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        // ignore and keep waiting
      }
    }
    return batchAllowed;
  }

  public void onResponseMsg(final BatchResMsg resMsg) {
    final boolean allow = resMsg.getAllow();
    batchAllowed = allow;
    latch.countDown();
  }
}
