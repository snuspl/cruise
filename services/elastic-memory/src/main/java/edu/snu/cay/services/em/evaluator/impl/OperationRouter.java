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

import edu.snu.cay.services.em.common.parameters.MemoryStoreId;
import edu.snu.cay.services.em.common.parameters.NumInitialEvals;
import edu.snu.cay.services.em.common.parameters.NumPartitions;
import edu.snu.cay.services.em.evaluator.api.PartitionFunc;
import edu.snu.cay.utils.LongRangeUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OperationRouter that redirects incoming operations on specific data ids to corresponding evaluators.
 */
@Private
public final class OperationRouter {

  private static final Logger LOG = Logger.getLogger(OperationRouter.class.getName());

  private String localEndPointId;

  private String evalPrefix;

  private final int memoryStoreId;

  private final PartitionFunc partitionFunc;

  /**
   * The number of partitions.
   */
  private final int numPartitions;

  /**
   * The number of initial Evaluators.
   */
  private final int numInitialEvals;

  /**
   * The location of partitions. It keeps just an index of MemoryStores,
   * so prefix should be added to get the Evaluator's endpoint id (See {@link #route(long)}).
   */
  private final int[] partitionToStore;
  private final Map<Integer, List<Integer>> storeToPartitions;

  // TODO #380: we have to improve router to provide different routing tables for each dataType.
  @Inject
  private OperationRouter(final PartitionFunc partitionFunc,
                          @Parameter(NumPartitions.class) final int numPartitions,
                          @Parameter(NumInitialEvals.class) final int numInitialEvals,
                          @Parameter(MemoryStoreId.class) final int memoryStoreId) {
    this.partitionFunc = partitionFunc;
    this.memoryStoreId = memoryStoreId;
    this.numPartitions = numPartitions;
    this.numInitialEvals = numInitialEvals;

    this.storeToPartitions = new HashMap<>(numInitialEvals);
    for (int storeIdx = 0; storeIdx < numInitialEvals; storeIdx++) {
      storeToPartitions.put(storeIdx, new LinkedList<Integer>());
    }

    this.partitionToStore = new int[numPartitions];
    for (int partitionIdx = 0; partitionIdx < numPartitions; partitionIdx++) {
      // Partitions are initially distributed across Evaluators in round-robin.
      final int storeIdx = partitionIdx % numInitialEvals;
      partitionToStore[partitionIdx] = storeIdx;
      storeToPartitions.get(storeIdx).add(partitionIdx);
    }
  }

  /**
   * Initialize the router.
   */
  public void initialize(final String endPointId) {
    this.localEndPointId = endPointId;
    this.evalPrefix = endPointId.split("-")[0];
    LOG.log(Level.INFO, "Initialize router with localEndPointId: {0}", localEndPointId);
  }

  /**
   * Decompose a list of key ranges into local ranges and remote ranges.
   * TODO #424: improve and optimize routing for range
   * @param dataKeyRanges a list of key ranges
   * @return a map composed of a partition id and a corresponding key range list and
   * a map composed of an endpoint id of remote evaluator and a corresponding key range list.
   */
  public Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> route(final List<LongRange> dataKeyRanges) {
    final Map<Integer, List<LongRange>> localKeyRangesMap = new HashMap<>();
    final Map<String, List<LongRange>> remoteKeyRangesMap = new HashMap<>();

    // perform routing for each dataKeyRanges of the operation
    for (final LongRange dataKeyRange : dataKeyRanges) {
      final Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> routingResult = route(dataKeyRange);

      final Map<Integer, List<LongRange>> partialLocalKeyRangesMap = routingResult.getFirst();
      // merge local sub ranges that targets same partition
      for (final Map.Entry<Integer, List<LongRange>> localEntry : partialLocalKeyRangesMap.entrySet()) {
        final List<LongRange> localRanges = localKeyRangesMap.get(localEntry.getKey());
        if (localRanges != null) {
          localRanges.addAll(localEntry.getValue());
        } else {
          localKeyRangesMap.put(localEntry.getKey(), localEntry.getValue());
        }
      }

      final Map<String, List<LongRange>> partialRemoteKeyRangesMap = routingResult.getSecond();
      // merge remote sub ranges that targets same evaluator
      for (final Map.Entry<String, List<LongRange>> remoteEntry : partialRemoteKeyRangesMap.entrySet()) {
        final List<LongRange> remoteRanges = remoteKeyRangesMap.get(remoteEntry.getKey());
        if (remoteRanges != null) {
          remoteRanges.addAll(remoteEntry.getValue());
        } else {
          remoteKeyRangesMap.put(remoteEntry.getKey(), remoteEntry.getValue());
        }
      }
    }
    return new Pair<>(localKeyRangesMap, remoteKeyRangesMap);
  }

  /**
   * Decompose a key range into local ranges and remote ranges.
   * @param dataKeyRange a key range
   * @return a map composed of a partition id and a corresponding key range list and
   * a map composed of an endpoint id of remote evaluator and a corresponding key range list.
   */
  public Pair<Map<Integer, List<LongRange>>, Map<String, List<LongRange>>> route(final LongRange dataKeyRange) {
    final Map<Integer, List<LongRange>> localKeyRanges = new HashMap<>();
    final Map<String, List<LongRange>> remoteKeyRanges = new HashMap<>();

    final Map<Integer, SortedSet<Long>> partitionedKeysMap = new HashMap<>();

    for (long dataKey = dataKeyRange.getMinimumLong(); dataKey <= dataKeyRange.getMaximumLong(); dataKey++) {
      final int partitionId = partitionFunc.getPartitionId(dataKey);
      if (!partitionedKeysMap.containsKey(partitionId)) {
        partitionedKeysMap.put(partitionId, new TreeSet<Long>());
      }
      final SortedSet<Long> dataKeys = partitionedKeysMap.get(partitionId);
      dataKeys.add(dataKey);
    }

    // translate ids to ranges
    for (final Map.Entry<Integer, SortedSet<Long>> partitionedKeysEntry : partitionedKeysMap.entrySet()) {
      final List<LongRange> rangeList =
          new ArrayList<>(LongRangeUtils.generateDenseLongRanges(partitionedKeysEntry.getValue()));
      final int partitionId = partitionedKeysEntry.getKey();
      final int targetStoreId = partitionToStore[partitionId];
      if (targetStoreId == memoryStoreId) {
        localKeyRanges.put(partitionId, rangeList);
      } else {
        // we assume that the values of store id and eval id mapped to each other are same.
        remoteKeyRanges.put(evalPrefix + '-' + targetStoreId, rangeList);
      }
    }

    return new Pair<>(localKeyRanges, remoteKeyRanges);
  }

  /**
   * Returns the routing result for the given {@code dataId}.
   * It returns the endpoint id of the evaluator that owns the data whose id is {@code dataId}.
   * A boolean value is piggybacked, which indicates whether the data is in the local memory store.
   * So the caller does not need to check that the target evaluator is local or not.
   *
   * @param dataId an id of data
   * @return a pair of an id of partition and and an Optional with an endpoint id of a target evaluator
   */
  public Pair<Integer, Optional<String>> route(final long dataId) {
    final int partitionId = partitionFunc.getPartitionId(dataId);
    final int targetEvalId = partitionToStore[partitionId];
    if (targetEvalId == memoryStoreId) {
      return new Pair<>(partitionId, Optional.<String>empty());
    } else {
      return new Pair<>(partitionId, Optional.of(evalPrefix + '-' + targetEvalId));
    }
  }

  public List<Integer> getPartitions() {
    return storeToPartitions.get(memoryStoreId);
  }
}
