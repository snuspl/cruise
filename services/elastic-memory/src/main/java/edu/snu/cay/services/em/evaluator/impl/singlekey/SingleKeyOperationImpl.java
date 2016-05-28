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
package edu.snu.cay.services.em.evaluator.impl.singlekey;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.evaluator.api.SingleKeyOperation;
import org.apache.reef.util.Optional;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of SingleKeyOperation.
 * It maintains metadata and states of the operation during execution.
 */
final class SingleKeyOperationImpl<K, V> implements SingleKeyOperation<K, V> {

  /**
   * Metadata of the operation.
   */
  private final Optional<String> origEvalId;
  private final String operationId;
  private final DataOpType operationType;
  private final K dataKey;
  private final Optional<V> dataValue;

  /**
   * States of the operation.
   */
  private CountDownLatch remoteOpCountDownLatch = new CountDownLatch(1);

  private final AtomicBoolean isSuccess = new AtomicBoolean(false);
  private final AtomicReference<Optional<V>> outputData = new AtomicReference<>(Optional.<V>empty());

  /**
   * A constructor for an operation.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataKey a key of data
   * @param dataValue an Optional with the value of data.
   *                  It is empty when the operation is one of GET or REMOVE.
   */
  public SingleKeyOperationImpl(final Optional<String> origEvalId, final String operationId,
                                final DataOpType operationType, final K dataKey,
                                final Optional<V> dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataKey = dataKey;
    this.dataValue = dataValue;
  }

  @Override
  public boolean isFromLocalClient() {
    return !origEvalId.isPresent();
  }

  @Override
  public Optional<String> getOrigEvalId() {
    return origEvalId;
  }

  @Override
  public String getOpId() {
    return operationId;
  }

  @Override
  public DataOpType getOpType() {
    return operationType;
  }

  @Override
  public K getKey() {
    return dataKey;
  }

  @Override
  public Optional<V> getValue() {
    return dataValue;
  }

  @Override
  public void commitResult(final Optional<V> output, final boolean success) {
    if (output.isPresent()) {
      outputData.set(output);
    }
    isSuccess.set(success);
    remoteOpCountDownLatch.countDown();
  }

  @Override
  public Optional<V> getOutputData() {
    return outputData.get();
  }

  @Override
  public boolean isSuccess() {
    return isSuccess.get();
  }

  @Override
  public boolean waitRemoteOps(final long timeout) throws InterruptedException {
    return remoteOpCountDownLatch.await(timeout, TimeUnit.MILLISECONDS);
  }
}
