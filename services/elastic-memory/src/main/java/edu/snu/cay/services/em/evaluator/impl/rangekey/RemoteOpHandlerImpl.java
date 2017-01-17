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
import edu.snu.cay.services.em.common.parameters.NumStoreThreads;
import edu.snu.cay.services.em.evaluator.api.DataOperation;
import edu.snu.cay.services.em.evaluator.api.RangeKeyOperation;
import edu.snu.cay.services.em.evaluator.api.RemoteOpHandler;
import edu.snu.cay.services.em.evaluator.impl.BlockStore;
import edu.snu.cay.services.em.evaluator.impl.OwnershipCache;
import edu.snu.cay.services.em.msg.api.EMMsgSender;
import edu.snu.cay.services.em.serialize.Serializer;
import edu.snu.cay.utils.Tuple3;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that executes all jobs related to remote access.
 * It 1) sends operation to remote stores and 2) sends the result of remote operation to the origin store,
 * and 3) receives and handles the received result.
 */
public final class RemoteOpHandlerImpl<K> implements RemoteOpHandler {
  private static final Logger LOG = Logger.getLogger(RemoteOpHandlerImpl.class.getName());
  private static final long TIMEOUT_MS = 40000;
  private static final int QUEUE_SIZE = 1024;
  private static final int QUEUE_TIMEOUT_MS = 3000;

  /**
   * A map holding ongoing operations until they finish.
   * It only maintains operations requested from local clients.
   */
  private final ConcurrentMap<String, RangeKeyOperation<K, Object>> ongoingOp = new ConcurrentHashMap<>();

  private final OwnershipCache ownershipCache;
  private final BlockStore blockStore;
  private final RangeSplitter<K> rangeSplitter;

  private Serializer serializer;
  private Codec<K> keyCodec;
  private final InjectionFuture<EMMsgSender> msgSender;

  /**
   * A queue for operations requested from remote clients.
   * Its element is composed of a operation, sub key ranges, and a corresponding block id.
   */
  private final BlockingQueue<Tuple3<RangeKeyOperation, List<Pair<K, K>>, Integer>> subOperationQueue
      = new ArrayBlockingQueue<>(QUEUE_SIZE);

  @Inject
  private RemoteOpHandlerImpl(final OwnershipCache ownershipCache,
                              final BlockStore blockStore,
                              final RangeSplitter<K> rangeSplitter,
                              final Serializer serializer,
                              @Parameter(KeyCodecName.class) final Codec<K> keyCodec,
                              @Parameter(NumStoreThreads.class) final int numStoreThreads,
                              final InjectionFuture<EMMsgSender> msgSender) {
    this.ownershipCache = ownershipCache;
    this.blockStore = blockStore;
    this.rangeSplitter = rangeSplitter;
    this.serializer = serializer;
    this.keyCodec = keyCodec;
    this.msgSender = msgSender;
    initExecutor(numStoreThreads);
  }

  /**
   * Initialize threads that dequeue and execute operation from the {@code subOperationQueue}.
   * That is, these threads serve operations requested from remote clients.
   */
  private void initExecutor(final int numStoreThreads) {
    final ExecutorService executor = Executors.newFixedThreadPool(numStoreThreads);
    for (int i = 0; i < numStoreThreads; i++) {
      executor.submit(new OperationThread());
    }
  }

  /**
   * A runnable that dequeues and executes operations requested from remote clients.
   * Several threads are initiated at the beginning and run as long-running background services.
   */
  private final class OperationThread implements Runnable {

    @Override
    public void run() {
      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final Tuple3<RangeKeyOperation, List<Pair<K, K>>, Integer> subOperation =
              subOperationQueue.poll(QUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (subOperation == null) {
            continue;
          }
          handleSubOperation(subOperation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
        }
      }
    }

    private void handleSubOperation(final Tuple3<RangeKeyOperation, List<Pair<K, K>>, Integer> subOperation) {
      final RangeKeyOperation operation = subOperation.getFirst();
      final List<Pair<K, K>> subKeyRanges = subOperation.getSecond();
      final int blockId = subOperation.getThird();

      LOG.log(Level.FINEST, "Poll op: [OpId: {0}, origId: {1}, block: {2}]]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get(), blockId});

      final Tuple<Optional<String>, Lock> remoteEvalIdWithLock = ownershipCache.resolveEvalWithLock(blockId);
      try {
        final Optional<String> remoteEvalIdOptional = remoteEvalIdWithLock.getKey();
        final boolean isLocalBlock = !remoteEvalIdOptional.isPresent();
        if (isLocalBlock) {
          final BlockImpl block = (BlockImpl) blockStore.get(blockId);
          final Map<K, Object> result = block.executeSubOperation(operation, subKeyRanges);
          submitLocalResult(operation, result, Collections.emptyList());
        } else {
          LOG.log(Level.WARNING,
              "Failed to execute operation {0} requested by remote store {2}. This store was considered as the owner" +
                  " of block {1} by store {2}, but the local ownership cache assumes store {3} is the owner",
              new Object[]{operation.getOpId(), blockId, operation.getOrigEvalId().get(), remoteEvalIdOptional.get()});

          // treat remote ranges as failed ranges, because we do not allow more than one hop in remote access
          final List<Pair<K, K>> failedRanges = new ArrayList<>(1);
          for (final Pair<K, K> subKeyRange : subKeyRanges) {
            failedRanges.add(new Pair<>(subKeyRange.getFirst(), subKeyRange.getSecond()));
          }
          submitLocalResult(operation, Collections.emptyMap(), failedRanges);
        }
      } finally {
        final Lock ownershipLock = remoteEvalIdWithLock.getValue();
        ownershipLock.unlock();
      }
    }
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

    final Codec<V> dataCodec = serializer.getCodec();

    // send sub operations to all remote stores that owns partial range of the main operation (RemoteOpMsg)
    for (final Map.Entry<String, List<Pair<K, K>>> evalToSubKeyRange : evalToSubKeyRangesMap.entrySet()) {
      try (TraceScope traceScope = Trace.startSpan("SEND_REMOTE_OP")) {
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
          dataKVPairList = Collections.emptyList();
        }

        msgSender.get().sendRemoteOpReqMsg(operation.getOrigEvalId().get(), targetEvalId,
            operation.getOpType(), avroKeyRangeList, dataKVPairList, operation.getOpId(), traceInfo);
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
   * Handles remote operation and its result.
   */
  @Override
  public void onNext(final RemoteOpMsg msg) {
    switch (msg.getType()) {
    case RemoteOpReqMsg:
      onRemoteOpReqMsg(msg);
      break;
    case RemoteOpResultMsg:
      onRemoteOpResultMsg(msg);
      break;
    default:
      throw new RuntimeException("Illegal msg type: " + msg.getType());
    }
  }

  /**
   * Handles the data operation sent from the remote memory store.
   */
  private void onRemoteOpReqMsg(final RemoteOpMsg msg) {
    final RemoteOpReqMsg remoteOpReqMsg = msg.getRemoteOpReqMsg();
    final String origEvalId = remoteOpReqMsg.getOrigEvalId().toString();
    final DataOpType operationType = remoteOpReqMsg.getOpType();
    final List<KeyRange> avroKeyRangeList = (List<KeyRange>) remoteOpReqMsg.getDataKeys();
    final List<KeyValuePair> dataKVPairList = (List<KeyValuePair>) remoteOpReqMsg.getDataValues();
    final String operationId = msg.getOperationId().toString();

    // decode data keys
    final List<Pair<K, K>> dataKeyRanges = new ArrayList<>(avroKeyRangeList.size());
    for (final KeyRange keyRange : avroKeyRangeList) {
      final K minKey = keyCodec.decode(keyRange.getMin().array());
      final K maxKey = keyCodec.decode(keyRange.getMax().array());
      dataKeyRanges.add(new Pair<>(minKey, maxKey));
    }

    // decode data values
    final Optional<NavigableMap<K, Object>> dataKeyValueMap;
    if (operationType.equals(DataOpType.PUT)) {
      final NavigableMap<K, Object> dataMap = new TreeMap<>();
      dataKeyValueMap = Optional.of(dataMap);

      final Codec dataCodec = serializer.getCodec();
      for (final KeyValuePair dataKVPair : dataKVPairList) {
        final K dataKey = keyCodec.decode(dataKVPair.getKey().array());
        final Object dataValue = dataCodec.decode(dataKVPair.getValue().array());
        dataMap.put(dataKey, dataValue);
      }
    } else {
      dataKeyValueMap = Optional.empty();
    }

    final DataOperation operation = new RangeKeyOperationImpl<>(Optional.of(origEvalId),
        operationId, operationType, dataKeyRanges, dataKeyValueMap);

    handleRemoteOp(operation);
  }

  /**
   * Handles operations requested from a remote client.
   */
  private void handleRemoteOp(final DataOperation dataOperation) {
    final RangeKeyOperation<K, Object> operation = (RangeKeyOperation<K, Object>) dataOperation;

    final Map<Integer, List<Pair<K, K>>> blockToSubKeyRangesMap =
        rangeSplitter.splitIntoSubKeyRanges(operation.getDataKeyRanges());

    // cannot resolve any block. invalid data keys
    if (blockToSubKeyRangesMap.isEmpty()) {
      // TODO #421: should handle fail case different from empty case
      submitLocalResult(operation, Collections.emptyMap(), operation.getDataKeyRanges());
      LOG.log(Level.SEVERE, "Failed Op [Id: {0}, origId: {1}]",
          new Object[]{operation.getOpId(), operation.getOrigEvalId().get()});
      return;
    }

    enqueueOperation(operation, blockToSubKeyRangesMap);
  }

  /**
   * Enqueues sub operations requested from a remote client to {@code subOperationQueue}.
   * The enqueued operations are executed by {@code OperationThread}s.
   */
  private void enqueueOperation(final RangeKeyOperation operation,
                                final Map<Integer, List<Pair<K, K>>> blockToKeyRangesMap) {
    final int numSubOps = blockToKeyRangesMap.size();
    operation.setNumSubOps(numSubOps);

    for (final Map.Entry<Integer, List<Pair<K, K>>> blockToSubKeyRanges : blockToKeyRangesMap.entrySet()) {
      final int blockId = blockToSubKeyRanges.getKey();
      final List<Pair<K, K>> keyRanges = blockToSubKeyRanges.getValue();

      try {
        subOperationQueue.put(new Tuple3<>(operation, keyRanges, blockId));
      } catch (final InterruptedException e) {
        LOG.log(Level.SEVERE, "Enqueue failed with InterruptedException", e);
      }

      LOG.log(Level.FINEST, "Enqueue Op [Id: {0}, block: {1}]",
          new Object[]{operation.getOpId(), blockId});
    }
  }

  /**
   * Handles the result of data operation processed by local memory store.
   * It waits until all sub operations are finished and their outputs are fully aggregated.
   */
  private <V> void submitLocalResult(final RangeKeyOperation<K, V> operation, final Map<K, V> localOutput,
                                     final List<Pair<K, K>> failedRanges) {
    final int numRemainingSubOps = operation.commitResult(localOutput, failedRanges);

    LOG.log(Level.FINE, "Local sub operation is finished. OpId: {0}, numRemainingSubOps: {1}",
        new Object[]{operation.getOpId(), numRemainingSubOps});

    if (numRemainingSubOps == 0) {
      sendResultToOrigin(operation);
    }
  }

  /**
   * Handles the result of data operation sent from the remote memory store.
   */
  private void onRemoteOpResultMsg(final RemoteOpMsg msg) {
    final RemoteOpResultMsg remoteOpResultMsg = msg.getRemoteOpResultMsg();
    final String operationId = msg.getOperationId().toString();
    final List<KeyValuePair> remoteOutput = (List<KeyValuePair>) remoteOpResultMsg.getDataValues();
    final List<KeyRange> failedAvroKeyRanges = remoteOpResultMsg.getFailedKeyRanges();

    final RangeKeyOperation<K, Object> operation = ongoingOp.get(operationId);

    if (operation == null) {
      LOG.log(Level.WARNING, "The operation is already handled or cancelled due to timeout. OpId: {0}", operationId);
      return;
    }

    final Codec codec = serializer.getCodec();

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
  private <V> void sendResultToOrigin(final RangeKeyOperation<K, V> operation) {

    LOG.log(Level.FINEST, "Send result to origin. OpId: {0}, OrigId: {1}",
        new Object[]{operation.getOpId(), operation.getOrigEvalId()});

    // send the original store the result (RemoteOpResultMsg)
    try (TraceScope traceScope = Trace.startSpan("SEND_REMOTE_RESULT")) {
      final Codec<V> dataCodec = serializer.getCodec();

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
        dataKVPairList = Collections.emptyList();
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
