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
package edu.snu.spl.cruise.ps.core.worker;

import edu.snu.spl.cruise.ps.PSMsg;
import edu.snu.spl.cruise.services.et.evaluator.api.TaskletCustomMsgHandler;
import edu.snu.spl.cruise.utils.AvroUtils;
import edu.snu.spl.cruise.utils.CatchableExecutors;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;

/**
 * A worker-side message handler that routes messages to an appropriate component corresponding to the msg type.
 */
public final class WorkerSideMsgHandler implements TaskletCustomMsgHandler {
  private static final int NUM_RELEASE_MSG_THREADS = 8;
  private static final int NUM_MODEL_EV_MSG_THREADS = 8;

  private final WorkerGlobalBarrier workerGlobalBarrier;
  private final ModelEvaluator modelEvaluator;

  private final ExecutorService releaseMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_RELEASE_MSG_THREADS);
  private final ExecutorService modelEvalMsgExecutor = CatchableExecutors.newFixedThreadPool(NUM_MODEL_EV_MSG_THREADS);

  @Inject
  private WorkerSideMsgHandler(final WorkerGlobalBarrier workerGlobalBarrier,
                               final ModelEvaluator modelEvaluator) {
    this.workerGlobalBarrier = workerGlobalBarrier;
    this.modelEvaluator = modelEvaluator;
  }

  @Override
  public void onNext(final byte[] bytes) {
    final PSMsg cruiseMsg = AvroUtils.fromBytes(bytes, PSMsg.class);
    switch (cruiseMsg.getType()) {
    case ReleaseMsg:
      releaseMsgExecutor.submit(workerGlobalBarrier::onReleaseMsg);
      break;

    case ModelEvalAnsMsg:
      modelEvalMsgExecutor.submit(() -> modelEvaluator.onMasterMsg(cruiseMsg.getModelEvalAnsMsg()));
      break;
    default:
      throw new RuntimeException("Unexpected msg type: " + cruiseMsg.getType());
    }
  }
}
