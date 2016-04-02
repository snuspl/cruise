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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.avro.AvroLongRange;
import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that handles the result of data operations both from local and remote memory stores.
 * The results are routed to a local client or an origin memory store where the operation is started.
 */
final class OperationResultAggregator {
  private static final Logger LOG = Logger.getLogger(OperationResultAggregator.class.getName());

  private final ConcurrentMap<String, DataOperation> ongoingOperations = new ConcurrentHashMap<>();

  private static final long TIMEOUT_MS = 40000;

  private final Serializer serializer;

  @Inject
  private OperationResultAggregator(final Serializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Registers an operation before sending it to remote memory store.
   * Registered operations would be removed by {@code submitResultAndWaitRemoteOps} method
   * when the operations are finished.
   */
  void registerOperation(final DataOperation operation, final int numSubOperations) {
    final DataOperation unhandledOperation = ongoingOperations.put(operation.getOperationId(), operation);
    operation.setCountDownLatch(numSubOperations);

    if (unhandledOperation != null) {
      LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}", unhandledOperation);
    }
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOperation(final String operationId) {
    ongoingOperations.remove(operationId);
  }

  /**
   * Handles the result of data operation that is processed by local memory store.
   * It waits
   * It returns the result to the local client or sends it to the origin evaluator of the data operation.
   */
  <T> void submitResultAndWaitRemoteOps(final DataOperation<T> operation, final Map<Long, T> localOutput) {
    operation.commitResult(localOutput, Collections.EMPTY_LIST);

    // wait until all sub operations are finished
    try {
      if (!operation.waitOperation(TIMEOUT_MS)) {
        LOG.log(Level.SEVERE, "operation timeout");
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
    } finally {
      deregisterOperation(operation.getOperationId());
    }
  }

  /**
   * Handles the result of data operation sent from remote memory store.
   * It always returns the result to the local client.
   */
  <T> void submitRemoteResult(final String operationId, final List<UnitIdPair> remoteOutput,
                              final List<AvroLongRange> failedRanges) {
    final DataOperation<T> operation = ongoingOperations.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    final Map<Long, T> dataKeyValueMap = new HashMap<>(remoteOutput.size());
    for (final UnitIdPair dataKeyValuePair : remoteOutput) {
      dataKeyValueMap.put(dataKeyValuePair.getId(), (T) codec.decode(dataKeyValuePair.getUnit().array()));
    }

    operation.commitResult(dataKeyValueMap, failedRanges);
  }
}
