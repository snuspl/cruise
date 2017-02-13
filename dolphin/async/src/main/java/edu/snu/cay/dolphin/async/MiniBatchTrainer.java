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

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yunseong on 2/12/17.
 */
public interface MiniBatchTrainer<D> {
  void initialize();

  void cleanup();

  void run(int idx, AtomicBoolean aborted);

  void runBatch(Collection<D> batchData, int epochIdx, int miniBatchIdx);

  void onEpochFinished(Collection<D> epochData, int epochIdx, int numMiniBatches, int numEMBlocks, long epochStartTime);
}
