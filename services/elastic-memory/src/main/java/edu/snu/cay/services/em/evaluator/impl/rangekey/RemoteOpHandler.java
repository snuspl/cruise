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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.services.em.utils.AvroUtils;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
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
 * A class that executes all jobs related to remote access.
 * It 1) sends operation to remote stores and 2) sends the result of remote operation to the origin store,
 * and 3) receives and handles the received result.
 */
final class RemoteOpHandler implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(RemoteOpHandler.class.getName());
  private static final long TIMEOUT_MS = 40000;

  /**
   * A map holding ongoing remote operations until they finish.
   */
  private final ConcurrentMap<String, LongKeyOperation> ongoingOp = new ConcurrentHashMap<>();

  private Serializer serializer;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  @Inject
  private RemoteOpHandler(final Serializer serializer,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.serializer = serializer;
    this.msgSender = msgSender;
  }

  /**
   * Send operation to remote evaluators.
   * @param operation an operation
   * @param evalToSubKeyRangesMap a map with an id of remote evaluator and a list of key ranges
   * @param <V> a type of data
   */
  <V> void sendOpToRemoteStores(final LongKeyOperation<V> operation,
                                final Map<String, List<Pair<Long, Long>>> evalToSubKeyRangesMap) {
    if (evalToSubKeyRangesMap.isEmpty()) {
      return;
    }

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}",
        new Object[]{operation.getOpId(), operation.getOpType()});

    registerOp(operation);

    final Codec codec = serializer.getCodec(operation.getDataType());

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<Pair<Long, Long>>> evalToSubKeyRange : evalToSubKeyRangesMap.entrySet()) {
      try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
        final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

        final String targetEvalId = evalToSubKeyRange.getKey();
        final List<Pair<Long, Long>> keyRangeList = evalToSubKeyRange.getValue();

        final List<UnitIdPair> dataKVPairList;
        if (operation.getOpType() == DataOpType.PUT) {
          final NavigableMap<Long, V> keyValueMap = operation.getDataKVMap().get();

          dataKVPairList = new LinkedList<>();

          for (final Pair<Long, Long> keyRange : keyRangeList) {

            // extract range-matching entries from the input data map
            final Map<Long, V> subMap = keyValueMap.subMap(keyRange.getFirst(), true,
                keyRange.getSecond(), true);

            // encode data values and put them into dataKVPairList
            for (final Map.Entry<Long, V> dataKVPair : subMap.entrySet()) {
              final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
              dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
            }
          }
        } else {
          // For GET and REMOVE operations, set dataKVPairList as an empty list
          dataKVPairList = Collections.EMPTY_LIST;
        }

        msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId().get(), targetEvalId,
            operation.getOpType(), operation.getDataType(), LongRangeUtils.fromPairsToRanges(keyRangeList),
            dataKVPairList, operation.getOpId(), traceInfo);
      }
    }

    try {
      if (!operation.waitRemoteOps(TIMEOUT_MS)) {
        LOG.log(Level.SEVERE, "Operation timeout. OpId: {0}", operation.getOpId());
      } else {
        LOG.log(Level.FINE, "Operation successfully finished. OpId: {0}", operation.getOpId());
      }
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for executing remote operation", e);
    } finally {
      deregisterOp(operation.getOpId());
    }
    // TODO #421: handle failures of operation (timeout, failed to locate).
  }

  /**
   * Handles the result of remote operation.
   */
  @Override
  public void onNext(final AvroElasticMemoryMessage msg) {

    final RemoteOpResultMsg remoteOpResultMsg = msg.getRemoteOpResultMsg();
    final String operationId = msg.getOperationId().toString();
    final List<UnitIdPair> remoteOutput = remoteOpResultMsg.getDataKVPairList();
    final List<AvroLongRange> failedAvroRanges = remoteOpResultMsg.getFailedKeyRanges();

    final LongKeyOperation operation = ongoingOp.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    final Map<Long, Object> dataKeyValueMap = new HashMap<>(remoteOutput.size());
    for (final UnitIdPair dataKeyValuePair : remoteOutput) {
      dataKeyValueMap.put(dataKeyValuePair.getId(), codec.decode(dataKeyValuePair.getUnit().array()));
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
   * Registers an operation before sending it to remote memory store.
   */
  private void registerOp(final LongKeyOperation operation) {
    final LongKeyOperation unhandledOperation = ongoingOp.put(operation.getOpId(), operation);
    if (unhandledOperation != null) {
      LOG.log(Level.SEVERE, "Discard the exceptionally unhandled operation: {0}",
          unhandledOperation.getOpId());
    }
  }

  /**
   * Deregisters an operation after its remote access is finished.
   */
  private void deregisterOp(final String operationId) {
    ongoingOp.remove(operationId);
  }

  /**
   * Sends the result to the original store.
   */
  <T> void sendResultToOrigin(final LongKeyOperation<T> operation) {

    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigEvalId()});

    // send the original store the result (RemoteOpResultMsg)
    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final String dataType = operation.getDataType();
      final Codec codec = serializer.getCodec(dataType);

      final Optional<String> origEvalId = operation.getOrigEvalId();
      final Map<Long, T> outputData = operation.getOutputData();

      final List<UnitIdPair> dataKVPairList;
      if (operation.getOpType() == DataOpType.GET || operation.getOpType() == DataOpType.REMOVE) {
        dataKVPairList = new ArrayList<>(outputData.size());

        for (final Map.Entry<Long, T> dataKVPair : outputData.entrySet()) {
          final ByteBuffer encodedData = ByteBuffer.wrap(codec.encode(dataKVPair.getValue()));
          dataKVPairList.add(new UnitIdPair(encodedData, dataKVPair.getKey()));
        }
      } else {
        dataKVPairList = Collections.EMPTY_LIST;
      }

      msgSender.get().sendRemoteOpResultMsg(origEvalId.get(), dataKVPairList, operation.getFailedKeyRanges(),
          operation.getOpId(), TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }
}
