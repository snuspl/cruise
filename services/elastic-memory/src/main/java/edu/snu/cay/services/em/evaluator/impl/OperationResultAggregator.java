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
import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.em.utils.AvroUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that handles the result of data operations both from local and remote memory stores.
 * The results are routed to a local client or an original memory store where the operation is started.
 */
final class OperationResultAggregator {
  private static final Logger LOG = Logger.getLogger(OperationResultAggregator.class.getName());

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<String, LongKeyOperation> ongoingOp = new ConcurrentHashMap<>();

  private static final long TIMEOUT_MS = 40000;

  private final Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  @Inject
  private OperationResultAggregator(final Serializer serializer,
                                    final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.serializer = serializer;
    this.msgSender = msgSender;
  }

  /**
   * Registers an operation before sending it to remote memory store.
   * Registered operations would be removed by {@code submitResultAndWaitRemoteOps} method
   * when the operations are finished.
   */
  void registerOp(final LongKeyOperation operation, final int numSubOperations) {

    if (operation.isFromLocalClient()) {
      final LongKeyOperation unhandledOperation = ongoingOp.put(operation.getOperationId(), operation);
      if (unhandledOperation != null) {
        LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}",
            unhandledOperation.getOperationId());
      }
    }

    operation.setNumSubOps(numSubOperations);
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOp(final String operationId) {
    ongoingOp.remove(operationId);
  }

  /**
   * Handles the result of data operation processed by local memory store.
   * It waits until all remote sub operations are finished and their outputs are fully aggregated.
   */
  <V> void submitLocalResult(final LongKeyOperation<V> operation, final Map<Long, V> localOutput,
                             final List<LongRange> failedRanges) {
    final int numRemainingSubOps = operation.commitResult(localOutput, failedRanges);

    LOG.log(Level.FINEST, "Local sub operation succeed. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operation.getOperationId(), numRemainingSubOps});

    if (!operation.isFromLocalClient()) {
      if (numRemainingSubOps == 0) {
        sendResultToOrigin(operation);
      }
    } else {
      // wait until all remote sub operations are finished
      try {
        if (!operation.waitOperation(TIMEOUT_MS)) {
          LOG.log(Level.SEVERE, "Operation timeout. OpId: {0}", operation.getOperationId());
        } else {
          LOG.log(Level.FINE, "Operation successfully finished. OpId: {0}", operation.getOperationId());
        }
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
      } finally {
        deregisterOp(operation.getOperationId());
      }
      // TODO #421: handle failures of operation (timeout, failed to locate).
    }
  }

  /**
   * Aggregates the result of data operation sent from remote memory store.
   */
  <V> void submitRemoteResult(final String operationId, final List<UnitIdPair> remoteOutput,
                              final List<AvroLongRange> failedAvroRanges) {

    final LongKeyOperation<V> operation = ongoingOp.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    final Map<Long, V> dataKeyValueMap = new HashMap<>(remoteOutput.size());
    for (final UnitIdPair dataKeyValuePair : remoteOutput) {
      dataKeyValueMap.put(dataKeyValuePair.getId(), (V) codec.decode(dataKeyValuePair.getUnit().array()));
    }

    final List<LongRange> failedRanges = new ArrayList<>(failedAvroRanges.size());
    for (final AvroLongRange avroRange : failedAvroRanges) {
      failedRanges.add(AvroUtils.fromAvroLongRange(avroRange));
    }

    final int numRemainingSubOps = operation.commitResult(dataKeyValueMap, failedRanges);

    LOG.log(Level.FINEST, "Remote sub operation succeed. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operationId, numRemainingSubOps});
  }

  /**
   * Sends the result to the original store.
   */
  private <T> void sendResultToOrigin(final LongKeyOperation<T> operation) {

    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOperationId(), operation.getOrigEvalId()});

    // send the original store the result (RemoteOpResultMsg)
    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final String dataType = operation.getDataType();
      final Codec codec = serializer.getCodec(dataType);

      final Optional<String> origEvalId = operation.getOrigEvalId();
      final Map<Long, T> outputData = operation.getOutputData();

      final List<UnitIdPair> dataKVPairList;
      if (operation.getOperationType() == DataOpType.GET || operation.getOperationType() == DataOpType.REMOVE) {
        dataKVPairList = new ArrayList<>(outputData.size());

        for (final Map.Entry<Long, T> dataKVPair : outputData.entrySet()) {
          final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
          dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
        }
      } else {
        dataKVPairList = Collections.EMPTY_LIST;
      }

      msgSender.get().sendRemoteOpResultMsg(origEvalId.get(), dataKVPairList, operation.getFailedRanges(),
          operation.getOperationId(), TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }
}
