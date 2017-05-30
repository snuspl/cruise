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

import edu.snu.cay.common.centcomm.avro.CentCommMsg;
import edu.snu.cay.utils.AvroUtils;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 *
 */
public final class WorkerSideMsgHandler implements EventHandler<CentCommMsg> {
  private final WorkerGlobalBarrier workerGlobalBarrier;

  @Inject
  private WorkerSideMsgHandler(final WorkerGlobalBarrier workerGlobalBarrier) {
    this.workerGlobalBarrier = workerGlobalBarrier;
  }

  @Override
  public synchronized void onNext(final CentCommMsg centCommMsg) {

    final byte[] data = centCommMsg.getData().array();

    final DolphinMsg dolphinMsg = AvroUtils.fromBytes(data, DolphinMsg.class);

    switch (dolphinMsg.getType()) {
    case ReleaseMsg:
      workerGlobalBarrier.onReleaseMsg();
      break;
    default:
      throw new RuntimeException("Unexpected msg type: " + dolphinMsg.getType());
    }
  }
}
