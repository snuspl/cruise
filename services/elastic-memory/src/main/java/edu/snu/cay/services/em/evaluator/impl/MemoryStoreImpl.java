/*
 * Copyright (C) 2015 Seoul National University
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

import edu.snu.cay.services.em.avro.DataOpType;
import edu.snu.cay.services.em.evaluator.api.RemoteAccessibleMemoryStore;
import edu.snu.cay.utils.trace.HTrace;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A {@code MemoryStore} implementation based on {@code TreeMap}s inside a single {@code HashMap}.
 * All data of one data type is stored in a {@code TreeMap}, ordered by data ids.
 * These {@code TreeMap}s are then maintained as values of one big {@code HashMap}, which uses the data types as keys.
 * A {@code ReentrantReadWriteLock} is used for synchronization between {@code get}, {@code put},
 * and {@code remove} operations.
  * Assuming EM applications always need to instantiate this class, HTrace initialization is done in the constructor.
 */
@EvaluatorSide
@Private
public final class MemoryStoreImpl implements RemoteAccessibleMemoryStore {
  private static final Logger LOG = Logger.getLogger(MemoryStoreImpl.class.getName());

  private static final int QUEUE_SIZE = 100;
  private static final int QUEUE_TIMOUT_MS = 3000;

  /**
   * This map uses data types, represented as strings, for keys and inner {@code TreeMaps} for values.
   * Each inner {@code TreeMap} serves as a collection of data of the same data type.
   * {@code TreeMap}s are used for guaranteeing log(n) read and write operations, especially
   * {@code getRange()} and {@code removeRange()} which are ranged queries based on the ids.
   */
  private final Map<String, TreeMap<Long, Object>> dataMap;

  /**
   * Used for synchronization between operations.
   * {@code get} uses the read lock, while {@code put} and {@code remove} use the write lock.
   */
  private final ReadWriteLock readWriteLock;

  private final OperationRouter router;
  private final OperationResultHandler resultHandler;
  private final RemoteOperationSender remoteSender;

  /**
   * A counter for issuing ids for operations requested from local clients.
   */
  private final AtomicLong operationIdCounter = new AtomicLong(0);

  /**
   * A queue for enqueueing operations from remote memory stores.
   */
  private final BlockingQueue<DataOperation> operationQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  @Inject
  private MemoryStoreImpl(final HTrace hTrace,
                          final OperationRouter router,
                          final OperationResultHandler resultHandler,
                          final RemoteOperationSender remoteSender) {
    hTrace.initialize();
    this.router = router;
    this.resultHandler = resultHandler;
    this.remoteSender = remoteSender;
    dataMap = new HashMap<>();
    readWriteLock = new ReentrantReadWriteLock(true);
    initialize();
  }

  private void initialize() {
    executorService.execute(new OperationThread());
  }

  @Override
  public void onNext(final DataOperation dataOperation) {
    try {
      operationQueue.put(dataOperation);
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Interrupted while waiting for enqueueing an operation", e);
    }
  }

  private final class OperationThread implements Runnable {

    private final int drainSize = QUEUE_SIZE / 10; // The max number of operations to drain per iteration
    private final ArrayList<DataOperation> drainedOperations = new ArrayList<>(drainSize);

    /**
     * A loop that dequeues operations and executes them.
     * Dequeues are only performed through this thread.
     */
    @Override
    public void run() {

      while (true) {
        // First, poll and execute a single operation.
        // Poll with a timeout will prevent busy waiting, when the queue is empty.
        try {
          final DataOperation operation = operationQueue.poll(QUEUE_TIMOUT_MS, TimeUnit.MILLISECONDS);
          if (operation == null) {
            continue;
          }
          executeOperation(operation);
        } catch (final InterruptedException e) {
          LOG.log(Level.SEVERE, "Poll failed with InterruptedException", e);
          continue;
        }

        // Then, drain up to drainSize of the remaining queue and execute.
        // drainTo method is much faster than multiple polls.
        if (operationQueue.drainTo(drainedOperations, drainSize) == 0) {
          continue;
        }

        for (final DataOperation operation : drainedOperations) {
          executeOperation(operation);
        }

        drainedOperations.clear();
      }
    }
  }

  private void executeOperation(final DataOperation operation) {
    final long dataKey = operation.getDataKey();

    final Pair<Boolean, String> routingResult = router.route(dataKey);

    final boolean isLocal = routingResult.getFirst();

    // 1) process local things or 2) send it off to remote memory store corresponding to the routing result
    if (isLocal) {
      executeLocalOperation(operation);
    } else {
      final String targetEvalId = routingResult.getSecond();
      remoteSender.sendOperation(targetEvalId, operation);
    }
  }

  private void executeLocalOperation(final DataOperation operation) {
    final DataOpType operationType = operation.getOperationType();
    final String dataType = operation.getDataType();
    final long key = operation.getDataKey();
    final Object value = operation.getInputData().get();

    final boolean result;
    final Object outputData;
    readWriteLock.writeLock().lock();
    try {
      switch (operationType) {
      case PUT:
        if (!dataMap.containsKey(dataType)) {
          dataMap.put(dataType, new TreeMap<Long, Object>());
        }
        dataMap.get(dataType).put(key, value);
        result = true;
        outputData = null;
        break;
      case GET:
        result = dataMap.containsKey(dataType);
        outputData = result ? dataMap.get(dataType).get(key) : null;
        break;
      case REMOVE:
        if (!dataMap.containsKey(dataType)) {
          result = false;
          outputData = null;
        } else {
          outputData = dataMap.get(dataType).remove(key);
          result = outputData != null;
        }
        break;
      default:
        throw new RuntimeException("Undefined operation");
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }

    resultHandler.handleLocalResult(operation, result, outputData);
  }

  @Override
  public <T> boolean put(final String dataType, final long id, final T value) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.PUT,
        dataType, id, Optional.of(value));

    executeOperation(operation);

    return operation.isSuccess();
  }

  // TODO #406: enable remote access
  @Override
  public <T> void putList(final String dataType, final List<Long> ids, final List<T> values) {
    if (ids.size() != values.size()) {
      throw new RuntimeException("Different list sizes: ids " + ids.size() + ", values " + values.size());
    }

    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        dataMap.put(dataType, new TreeMap<Long, Object>());
      }
      final Map<Long, Object> innerMap = dataMap.get(dataType);

      for (int index = 0; index < ids.size(); index++) {
        innerMap.put(ids.get(index), values.get(index));
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public <T> Pair<Long, T> get(final String dataType, final long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.GET,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    return new Pair<>(id, operation.getOutputData().get());
  }

  // TODO #406: enable remote access
  @Override
  public <T> Map<Long, T> getAll(final String dataType) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      return (Map<Long, T>)dataMap.get(dataType).clone();

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  // TODO #406: enable remote access
  @Override
  public <T> Map<Long, T> getRange(final String dataType, final long startId, final long endId) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      return new TreeMap<>((SortedMap<Long, T>)dataMap.get(dataType).subMap(startId, true, endId, true));

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public <T> Pair<Long, T> remove(final String dataType, final long id) {

    final String operationId = Long.toString(operationIdCounter.getAndIncrement());
    final DataOperation<T> operation = new DataOperation<>(Optional.<String>empty(), operationId, DataOpType.REMOVE,
        dataType, id, Optional.<T>empty());

    executeOperation(operation);

    return new Pair<>(id, operation.getOutputData().get());
  }

  // TODO #406: enable remote access
  @Override
  public <T> Map<Long, T> removeAll(final String dataType) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      return (Map<Long, T>)dataMap.remove(dataType);

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // TODO #406: enable remote access
  @Override
  public <T> Map<Long, T> removeRange(final String dataType, final long startId, final long endId) {
    readWriteLock.writeLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return new TreeMap<>();
      }
      final TreeMap<Long, T> subMap =
          new TreeMap<>((SortedMap<Long, T>)dataMap.get(dataType).subMap(startId, true, endId, true));
      dataMap.get(dataType).keySet().removeAll(subMap.keySet());
      return subMap;

    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public Set<String> getDataTypes() {
    readWriteLock.readLock().lock();

    try {
      final Set<String> dataTypeSet = new HashSet<>(dataMap.keySet().size());
      for (final Map.Entry<String, TreeMap<Long, Object>> entry : dataMap.entrySet()) {
        if (!entry.getValue().isEmpty()) {
          dataTypeSet.add(entry.getKey());
        }
      }

      return dataTypeSet;

    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  @Override
  public int getNumUnits(final String dataType) {
    readWriteLock.readLock().lock();

    try {
      if (!dataMap.containsKey(dataType)) {
        return 0;
      }
      return dataMap.get(dataType).size();

    } finally {
      readWriteLock.readLock().unlock();
    }
  }
}
