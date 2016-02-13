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
package edu.snu.cay.services.ps.worker.partitioned;

import edu.snu.cay.services.ps.worker.AsyncWorkerHandler;

import javax.inject.Inject;

public final class PartitionedWorkerHandler<K, V> implements AsyncWorkerHandler<K, V> {
  private final PartitionedParameterWorker<K, ?, V> partitionedParameterWorker;

  @Inject
  private PartitionedWorkerHandler(final PartitionedParameterWorker<K, ?, V> partitionedParameterWorker) {
    this.partitionedParameterWorker = partitionedParameterWorker;
  }

  @Override
  public void processReply(final K key, final V value) {
    partitionedParameterWorker.processReply(key, value);
  }
}
