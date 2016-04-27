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
package edu.snu.cay.services.em.evaluator.impl.rangekey;

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.evaluator.api.RangeKeyOperation;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of RangeKeyOperation.
 * It maintains metadata and states of the operation during execution.
 */
@Private
final class RangeKeyOperationImpl<K, V> implements RangeKeyOperation<K, V> {

  /**
   * Metadata of the operation.
   */
  private final Optional<String> origEvalId;
  private final String operationId;
  private final DataOpType operationType;
  private final String dataType;
  private final List<Pair<K, K>> dataKeyRanges;
  private final Optional<NavigableMap<K, V>> dataKeyValueMap;

  /**
   * States of the operation.
   */
  // default is 1 for operations failed even before being separated into sub operations
  private final AtomicInteger subOpCounter = new AtomicInteger(1);
  private CountDownLatch remoteOpCountDownLatch = new CountDownLatch(0);

  // ranges that remote sub operations failed to execute due to wrong routing
  // it happens only when ownership of data key are updated, unknown to the original store
  private final List<Pair<K, K>> failedRanges = Collections.synchronizedList(new LinkedList<Pair<K, K>>());
  private final ConcurrentMap<K, V> outputData = new ConcurrentHashMap<>();

  /**
   * A constructor for an operation composed of multiple data key ranges.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKeyRanges a list of data key ranges
   * @param dataKeyValueMap an Optional with the map of the data keys and data values.
   *                        It is empty when the operation is one of GET or REMOVE.
   */
  public RangeKeyOperationImpl(final Optional<String> origEvalId, final String operationId,
                               final DataOpType operationType, final String dataType,
                               final List<Pair<K, K>> dataKeyRanges,
                               final Optional<NavigableMap<K, V>> dataKeyValueMap) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;
    this.dataKeyRanges = dataKeyRanges;
    this.dataKeyValueMap = dataKeyValueMap;
  }

  /**
   * A constructor for an operation composed of a single data key range.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKeyRange a range of data keys
   * @param dataKeyValueMap an Optional with the map of the data keys and data values.
   *                        It is empty when the operation is one of GET or REMOVE.
   */
  public RangeKeyOperationImpl(final Optional<String> origEvalId, final String operationId,
                               final DataOpType operationType, final String dataType,
                               final Pair<K, K> dataKeyRange,
                               final Optional<NavigableMap<K, V>> dataKeyValueMap) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;

    final List<Pair<K, K>> keyRanges = new ArrayList<>(1);
    keyRanges.add(dataKeyRange);
    this.dataKeyRanges = keyRanges;

    this.dataKeyValueMap = dataKeyValueMap;
  }

  /**
   * A constructor for an operation composed of a single data key.
   * @param origEvalId an Optional with the id of the original evaluator where the operation is generated.
   *                   It is empty when the operation is requested from the local client.
   * @param operationId an id of operation
   * @param operationType a type of operation
   * @param dataType a type of data
   * @param dataKey a key of data
   * @param dataValue an Optional with the value of data.
   *                  It is empty when the operation is one of GET or REMOVE.
   */
  public RangeKeyOperationImpl(final Optional<String> origEvalId, final String operationId,
                               final DataOpType operationType, final String dataType, final K dataKey,
                               final Optional<V> dataValue) {
    this.origEvalId = origEvalId;
    this.operationId = operationId;
    this.operationType = operationType;
    this.dataType = dataType;

    final List<Pair<K, K>> keyRanges = new ArrayList<>(1);
    keyRanges.add(new Pair<>(dataKey, dataKey));
    this.dataKeyRanges = keyRanges;

    final NavigableMap<K, V> keyValueMap;
    if (dataValue.isPresent()) {
      keyValueMap = new TreeMap<>();
      keyValueMap.put(dataKey, dataValue.get());
      this.dataKeyValueMap = Optional.of(keyValueMap);
    } else {
      this.dataKeyValueMap = Optional.empty();
    }
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
  public String getDataType() {
    return dataType;
  }

  /**
   * The method assumes that callers never try to modify the returned object.
   */
  @Override
  public List<Pair<K, K>> getDataKeyRanges() {
    return dataKeyRanges;
  }

  /**
   * The method assumes that callers never try to modify the returned object.
   */
  @Override
  public Optional<NavigableMap<K, V>> getDataKVMap() {
    return dataKeyValueMap;
  }

  /**
   * Sets the total number of sub operations for an atomic counter. For operations from local clients,
   * it also initializes a latch that {@link #waitRemoteOps(long)} will wait until the count becomes zero.
   */
  @Override
  public void setNumSubOps(final int numSubOps) {
    if (numSubOps == 0) {
      return;
    }

    if (isFromLocalClient()) {
      remoteOpCountDownLatch = new CountDownLatch(numSubOps);
    }
    subOpCounter.set(numSubOps);
  }

  @Override
  public boolean waitRemoteOps(final long timeout) throws InterruptedException {
    return remoteOpCountDownLatch.await(timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public int commitResult(final Map<K, V> output, final List<Pair<K, K>> failedRangeList) {
    this.outputData.putAll(output);
    this.failedRanges.addAll(failedRangeList);

    if (isFromLocalClient()) {
      remoteOpCountDownLatch.countDown();
    }
    return subOpCounter.decrementAndGet();
  }

  @Override
  public Map<K, V> getOutputData() {
    return outputData;
  }

  @Override
  public List<Pair<K, K>> getFailedKeyRanges() {
    return failedRanges;
  }
}
