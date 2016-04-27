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
import edu.snu.cay.services.em.common.parameters.KeyCodecName;
import edu.snu.cay.services.em.evaluator.api.RangeKeyOperation;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
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
final class RemoteOpHandler<K> implements EventHandler<AvroElasticMemoryMessage> {
  private static final Logger LOG = Logger.getLogger(RemoteOpHandler.class.getName());
  private static final long TIMEOUT_MS = 40000;

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<String, RangeKeyOperation<K, Object>> ongoingOp = new ConcurrentHashMap<>();

  private Serializer serializer;
  private Codec<K> keyCodec;
  private final InjectionFuture<ElasticMemoryMsgSender> msgSender;

  @Inject
  private RemoteOpHandler(final Serializer serializer,
                          @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                          final InjectionFuture<ElasticMemoryMsgSender> msgSender) {
    this.serializer = serializer;
    this.keyCodec = keyCodec;
    this.msgSender = msgSender;
  }

  /**
   * Send operation to remote evaluators.
   * @param operation an operation
   * @param evalToSubKeyRangesMap a map with an id of remote evaluator and a list of key ranges
   * @param <V> a type of data
   */
  <V> void sendOpToRemoteStores(final RangeKeyOperation<K, V> operation,
                            final Map<String, List<Pair<K, K>>> evalToSubKeyRangesMap) {
    if (evalToSubKeyRangesMap.isEmpty()) {
      return;
    }

    LOG.log(Level.FINEST, "Send op to remote. OpId: {0}, OpType: {1}",
        new Object[]{operation.getOpId(), operation.getOpType()});

    registerOp(operation);

    final Codec<V> dataCodec = serializer.getCodec(operation.getDataType());

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<Pair<K, K>>> evalToSubKeyRange : evalToSubKeyRangesMap.entrySet()) {
      try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
        final TraceInfo traceInfo = TraceInfo.fromSpan(traceScope.getSpan());

        final String targetEvalId = evalToSubKeyRange.getKey();
        final List<Pair<K, K>> keyRangeList = evalToSubKeyRange.getValue();

        // encode key range
        final List<KeyRange> avroKeyRangeList = new ArrayList<>(keyRangeList.size());
        for (final Pair<K, K> keyRange : keyRangeList) {
          final ByteBuffer encodedMinKey = ByteBuffer.wrap(keyCodec.encode(keyRange.getFirst()));
          final ByteBuffer encodedMaxKey = ByteBuffer.wrap(keyCodec.encode(keyRange.getSecond()));
          final KeyRange avroKeyRange = new KeyRange(encodedMinKey, encodedMaxKey);
          avroKeyRangeList.add(avroKeyRange);
        }

        //encode data value
        final List<KeyValuePair> dataKVPairList;
        if (operation.getOpType() == DataOpType.PUT) {
          final NavigableMap<K, V> keyValueMap = operation.getDataKVMap().get();

          dataKVPairList = new LinkedList<>();

          for (final Pair<K, K> keyRange : keyRangeList) {
            // extract range-matching entries from the input data map
            final Map<K, V> subMap = keyValueMap.subMap(keyRange.getFirst(), true,
                keyRange.getSecond(), true);

            // encode dataKeyValue and put them into dataKVPairList
            for (final Map.Entry<K, V> dataKVPair : subMap.entrySet()) {
              final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(dataKVPair.getKey()));
              final ByteBuffer encodedData = ByteBuffer.wrap(dataCodec.encode(dataKVPair.getValue()));
              dataKVPairList.add(new KeyValuePair(encodedKey, encodedData));
            }
          }
        } else {
          // For GET and REMOVE operations, set dataKVPairList as an empty list
          dataKVPairList = Collections.EMPTY_LIST;
        }

        msgSender.get().sendRemoteOpMsg(operation.getOrigEvalId().get(), targetEvalId,
            operation.getOpType(), operation.getDataType(), avroKeyRangeList,
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
    final List<KeyValuePair> remoteOutput = (List<KeyValuePair>) remoteOpResultMsg.getDataValues();
    final List<KeyRange> failedAvroKeyRanges = remoteOpResultMsg.getFailedKeyRanges();

    final RangeKeyOperation<K, Object> operation = ongoingOp.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    final Codec codec = serializer.getCodec(operation.getDataType());

    // decode data
    final Map<K, Object> dataKeyValueMap = new HashMap<>(remoteOutput.size());
    for (final KeyValuePair dataKeyValuePair : remoteOutput) {
      final K dataKey = keyCodec.decode(dataKeyValuePair.getKey().array());
      final Object dataValue = codec.decode(dataKeyValuePair.getValue().array());
      dataKeyValueMap.put(dataKey, dataValue);
    }

    // decode failed data key ranges
    final List<Pair<K, K>> failedKeyRanges = new ArrayList<>(failedAvroKeyRanges.size());
    for (final KeyRange avroKeyRange : failedAvroKeyRanges) {
      final K minKey = keyCodec.decode(avroKeyRange.getMin().array());
      final K maxKey = keyCodec.decode(avroKeyRange.getMax().array());
      failedKeyRanges.add(new Pair<>(minKey, maxKey));
    }

    final int numRemainingSubOps = operation.commitResult(dataKeyValueMap, failedKeyRanges);

    LOG.log(Level.FINEST, "Remote sub operation succeed. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operationId, numRemainingSubOps});
  }

  /**
   * Registers an operation before sending it to remote memory store.
   */
  private void registerOp(final RangeKeyOperation operation) {
    final RangeKeyOperation unhandledOperation = ongoingOp.put(operation.getOpId(), operation);
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
  <V> void sendResultToOrigin(final RangeKeyOperation<K, V> operation) {

    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigEvalId()});

    // send the original store the result (RemoteOpResultMsg)
    try (final TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final String dataType = operation.getDataType();
      final Codec<V> dataCodec = serializer.getCodec(dataType);

      final Optional<String> origEvalId = operation.getOrigEvalId();

      // encode output data
      final Map<K, V> outputData = operation.getOutputData();
      final List<KeyValuePair> dataKVPairList;
      if (operation.getOpType() == DataOpType.GET || operation.getOpType() == DataOpType.REMOVE) {
        dataKVPairList = new ArrayList<>(outputData.size());

        for (final Map.Entry<K, V> dataKVPair : outputData.entrySet()) {
          final ByteBuffer encodedKey = ByteBuffer.wrap(keyCodec.encode(dataKVPair.getKey()));
          final ByteBuffer encodedData = ByteBuffer.wrap(dataCodec.encode(dataKVPair.getValue()));
          dataKVPairList.add(new KeyValuePair(encodedKey, encodedData));
        }
      } else {
        dataKVPairList = Collections.EMPTY_LIST;
      }

      // encoded failed key ranges
      final List<Pair<K, K>> failedKeyRangeList = operation.getFailedKeyRanges();
      final List<KeyRange> avroFailedKeyRangeList = new ArrayList<>(failedKeyRangeList.size());
      for (final Pair<K, K> keyRange : failedKeyRangeList) {
        final ByteBuffer encodedMinKey = ByteBuffer.wrap(keyCodec.encode(keyRange.getFirst()));
        final ByteBuffer encodedMaxKey = ByteBuffer.wrap(keyCodec.encode(keyRange.getSecond()));
        final KeyRange avroKeyRange = new KeyRange(encodedMinKey, encodedMaxKey);
        avroFailedKeyRangeList.add(avroKeyRange);
      }

      msgSender.get().sendRemoteOpResultMsg(origEvalId.get(), dataKVPairList, avroFailedKeyRangeList,
          operation.getOpId(), TraceInfo.fromSpan(traceScope.getSpan()));
    }
  }
}
