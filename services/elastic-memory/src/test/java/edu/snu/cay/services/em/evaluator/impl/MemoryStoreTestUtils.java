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

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.evaluator.api.MemoryStore;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.util.Optional;
import org.htrace.HTraceConfiguration;
import org.htrace.Span;
import org.htrace.SpanReceiver;
import org.htrace.TraceInfo;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Utilities for testing MemoryStore.
 */
public final class MemoryStoreTestUtils {

  public enum IndexParity {
    EVEN_INDEX, ODD_INDEX, ALL_INDEX
  }

  public static final class PutThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final String dataType;
    private final int myIndex;
    private final int numThreads;
    private final int putsPerThread;
    private final int itemsPerPut;
    private final IndexParity indexParity;

    public PutThread(final CountDownLatch countDownLatch,
                     final MemoryStore memoryStore, final String dataType,
                     final int myIndex, final int numThreads, final int putsPerThread, final int itemsPerPut,
                     final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.dataType = dataType;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.putsPerThread = putsPerThread;
      this.itemsPerPut = itemsPerPut;
      this.indexParity = indexParity;
    }

    @Override
    public void run() {
      for (int i = 0; i < putsPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1) {
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        if (itemsPerPut == 1) {
          final long itemIndex = numThreads * i + myIndex;
          memoryStore.put(dataType, itemIndex, i);
        } else {
          final long itemStartIndex = (numThreads * i + myIndex) * itemsPerPut;
          final List<Long> ids = new ArrayList<>(itemsPerPut);
          final List<Long> values = new ArrayList<>(itemsPerPut);
          for (long itemIndex = itemStartIndex; itemIndex < itemStartIndex + itemsPerPut; itemIndex++) {
            ids.add(itemIndex);
            values.add(itemIndex);
          }
          memoryStore.putList(dataType, ids, values);
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class RemoveThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final String dataType;
    private final int myIndex;
    private final int numThreads;
    private final int removesPerThread;
    private final int itemsPerRemove;
    private final IndexParity indexParity;

    public RemoveThread(final CountDownLatch countDownLatch,
                        final MemoryStore memoryStore, final String dataType,
                        final int myIndex, final int numThreads, final int removesPerThread, final int itemsPerRemove,
                        final IndexParity indexParity) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.dataType = dataType;
      this.myIndex = myIndex;
      this.numThreads = numThreads;
      this.removesPerThread = removesPerThread;
      this.itemsPerRemove = itemsPerRemove;
      this.indexParity = indexParity;
    }

    @Override
    public void run() {
      for (int i = 0; i < removesPerThread; i++) {
        if (indexParity == IndexParity.EVEN_INDEX && i % 2 == 1) {
          continue;
        }
        if (indexParity == IndexParity.ODD_INDEX && i % 2 == 0) {
          continue;
        }

        if (itemsPerRemove == 1) {
          final long itemIndex = numThreads * i + myIndex;
          memoryStore.remove(dataType, itemIndex);
        } else {
          final long itemStartIndex = (numThreads * i + myIndex) * itemsPerRemove;
          final long itemEndIndex = itemStartIndex + itemsPerRemove - 1;
          memoryStore.removeRange(dataType, itemStartIndex, itemEndIndex);
        }
      }

      countDownLatch.countDown();
    }
  }

  public static final class GetThread implements Runnable {
    private final CountDownLatch countDownLatch;
    private final MemoryStore memoryStore;
    private final String dataType;
    private final int getsPerThread;
    private final int totalNumberOfObjects;
    private final boolean testRange;
    private final Random random;

    public GetThread(final CountDownLatch countDownLatch,
                     final MemoryStore memoryStore, final String dataType,
                     final int getsPerThread, final boolean testRange,
                     final int totalNumberOfObjects) {
      this.countDownLatch = countDownLatch;
      this.memoryStore = memoryStore;
      this.dataType = dataType;
      this.getsPerThread = getsPerThread;
      this.testRange = testRange;
      this.totalNumberOfObjects = totalNumberOfObjects;
      this.random = new Random();
    }

    @Override
    public void run() {
      for (int i = 0; i < getsPerThread; i++) {
        final int getMethod = testRange ? random.nextInt(3) : 0;
        if (getMethod == 0) {
          memoryStore.get(dataType, (long) random.nextInt(totalNumberOfObjects));

        } else if (getMethod == 1) {
          final int startId = random.nextInt(totalNumberOfObjects);
          final int endId = random.nextInt(totalNumberOfObjects - startId) + startId;

          final Map<Long, Object> subMap = memoryStore.getRange(dataType, (long) startId, (long) endId);
          if (subMap == null) {
            continue;
          }

          // We make sure this thread actually iterates over the returned map, so that
          // we can check if other threads writing on the backing map affect this thread.
          for (final Map.Entry entry : subMap.entrySet()) {
            entry.getKey();
          }

        } else {
          final Map<Long, Object> allMap = memoryStore.getAll(dataType);
          if (allMap == null) {
            continue;
          }

          // We make sure this thread actually iterates over the returned map, so that
          // we can check if other threads writing on the backing map affect this thread.
          for (final Map.Entry entry : allMap.entrySet()) {
            entry.getKey();
          }
        }
      }

      countDownLatch.countDown();
    }
  }

  /**
   * Mocked message sender that implements ElasticMemoryMsgSender, which is required to instantiate MemoryStore.
   */
  public static final class MockedMsgSender implements ElasticMemoryMsgSender {

    @Inject
    private MockedMsgSender() {

    }

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final List<KeyRange> dataKeyRanges,
                                final List<KeyValuePair> dataKVPairList, final String operationId,
                                @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpMsg(final String origId, final String destId, final DataOpType operationType,
                                final String dataType, final DataKey dataKey, final DataValue dataValue,
                                final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final List<KeyValuePair> dataKVPairList,
                                      final List<KeyRange> failedRanges, final String operationId,
                                      @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRemoteOpResultMsg(final String destId, final DataValue dataValue, final boolean isSuccess,
                                      final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingInitRequestMsg(@Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingInitMsg(final String destId, final List<Integer> blockLocations,
                                   @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingUpdateMsg(final String destId, final List<Integer> blocks,
                                     final String oldOwnerId, final String newOwnerId,
                                     @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRoutingUpdateAckMsg(@Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                            final Set<LongRange> idRangeSet, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId, final int numUnits,
                            final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                            final List<Integer> blocks, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendDataMsg(final String destId, final String dataType, final List<UnitIdPair> unitIdPairList,
                            final int blockId, final String operationId, @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendDataAckMsg(final Set<LongRange> idRangeSet, final String operationId,
                               @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendRegisMsg(final String dataType, final long unitStartId, final long unitEndId,
                             @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendUpdateMsg(final String destId, final String operationId,
                              @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendUpdateAckMsg(final String operationId, final UpdateResult result,
                                 @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendOwnershipMsg(final Optional<String> destId, final String operationId, final String dataType,
                                 final int blockId, final int oldOwnerId, final int newOwnerId,
                                 @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendOwnershipAckMsg(final String operationId, final String dataType, final int blockId,
                                    @Nullable final TraceInfo parentTraceInfo) {

    }

    @Override
    public void sendFailureMsg(final String operationId, final String reason,
                               @Nullable final TraceInfo parentTraceInfo) {

    }
  }

  /**
   * Mocked span receiver that implements SpanReceiver, which is required to instantiate HTrace.
   * In this test, the instantiated HTrace is eventually used to instantiate MemoryStore.
   */
  public static final class MockedSpanReceiver implements SpanReceiver {

    @Inject
    private MockedSpanReceiver() {

    }

    @Override
    public void configure(final HTraceConfiguration hTraceConfiguration) {

    }

    @Override
    public void receiveSpan(final Span span) {

    }

    @Override
    public void close() throws IOException {

    }
  }
}


