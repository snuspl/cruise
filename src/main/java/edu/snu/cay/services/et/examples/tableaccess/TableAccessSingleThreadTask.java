/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.services.et.examples.tableaccess;

import com.google.common.collect.Lists;
import edu.snu.cay.services.et.configuration.parameters.ETIdentifier;
import edu.snu.cay.services.et.configuration.parameters.ExecutorIdentifier;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.evaluator.impl.OrderingBasedBlockPartitioner;
import edu.snu.cay.services.et.examples.tableaccess.parameters.KeyOffsetByExecutor;
import edu.snu.cay.services.et.examples.tableaccess.parameters.NumExecutorsToRunTask;
import edu.snu.cay.services.et.examples.tableaccess.parameters.TableIdentifier;
import edu.snu.cay.services.et.examples.tableaccess.parameters.BlockAccessType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.cay.services.et.examples.tableaccess.TableAccessETDriver.*;
import static edu.snu.cay.services.et.examples.tableaccess.PrefixUpdateFunction.UPDATE_PREFIX;

/**
 * Table access task with a single thread.
 * Task code runs a test with PUT, GET, UPDATE, DELETE operations,
 * and checks that all operations are executed correctly.
 */
public final class TableAccessSingleThreadTask implements Task {
  private static final Logger LOG = Logger.getLogger(TableAccessSingleThreadTask.class.getName());

  // Tests with all block access pattern will access each block exactly once
  private static final int NUM_OPERATIONS = NUM_BLOCKS;
  private static final int NUM_MULTI_OPERATIONS = 10;

  // block id to access in ONE_BLOCK access test
  private static final int BLOCK_ID_ACCESS = 0;

  private final ExecutorSynchronizer executorSynchronizer;
  private final TableAccessor tableAccessor;
  private final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner;

  private final String elasticTableId;
  private final String executorId;
  private final String tableId;
  private final String blockAccessType;
  private final int keyOffsetByExecutor;
  private final int numExecutorsToRunTask;

  private final Long[] randomKeyArray;

  private List<Pair<String, Long>> testNameToTimeList = new LinkedList<>();
  private final AtomicInteger testCounter = new AtomicInteger(0);

  @Inject
  private TableAccessSingleThreadTask(final ExecutorSynchronizer executorSynchronizer,
                                      final TableAccessor tableAccessor,
                                      final OrderingBasedBlockPartitioner orderingBasedBlockPartitioner,
                                      @Parameter(ETIdentifier.class) final String elasticTableId,
                                      @Parameter(ExecutorIdentifier.class) final String executorId,
                                      @Parameter(TableIdentifier.class) final String tableId,
                                      @Parameter(BlockAccessType.class) final String blockAccessType,
                                      @Parameter(KeyOffsetByExecutor.class) final int keyOffsetByExecutor,
                                      @Parameter(NumExecutorsToRunTask.class) final int numExecutorsToRunTask) {

    this.executorSynchronizer = executorSynchronizer;
    this.tableAccessor = tableAccessor;
    this.orderingBasedBlockPartitioner = orderingBasedBlockPartitioner;
    this.elasticTableId = elasticTableId;
    this.executorId = executorId;
    this.tableId = tableId;
    this.blockAccessType = blockAccessType;
    this.keyOffsetByExecutor = keyOffsetByExecutor;
    this.numExecutorsToRunTask = numExecutorsToRunTask;
    this.randomKeyArray = getRandomizedKeyArray();
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "Hello, {0}! I am an executor id {1}", new Object[]{elasticTableId, executorId});

    final Table<Long, String, String> table = tableAccessor.getTable(tableId);
    LOG.log(Level.INFO, "Table id: {0}", tableId);

    final long startTime = System.currentTimeMillis();
    // 1. put, get test
    // gets data from table (it should be empty)
    runTest(new GetTest(table, false, true));
    // puts data in table (puts will succeed and return values would be null)
    runTest(new PutTest(table, false, true));
    // puts data in table (overwrite table, return values are the ones that put by previous puts)
    runTest(new PutTest(table, false, false));
    // gets data from table (value is existed)
    runTest(new GetTest(table, false, false));
    runTest(new MultiPutTest(table, false, false));

    // 2. update test
    runTest(new UpdateTest(table));
    // gets data from table (whether to check the value is updated)
    runTest(new GetTest(table, true, false));
    // puts data in table (value is back to init put data)
    runTest(new PutTest(table, true, false));
    runTest(new GetTest(table, false, false));
    runTest(new MultiUpdateTest(table));

    // 3. remove test
    runTest(new RemoveTest(table, true, false));
    runTest(new GetTest(table, false, true));

    final long endTime = System.currentTimeMillis();
    testNameToTimeList.add(Pair.of("Total test time", endTime - startTime));
    printResult();
    return null;
  }

  private void runTest(final Runnable test) {
    final long startTime = System.currentTimeMillis();
    final int testIndex = testCounter.getAndIncrement();
    LOG.log(Level.INFO, "Test start: {0} in test no. {1}",
        new Object[]{test.toString(), testIndex});

    test.run();

    final long endTime = System.currentTimeMillis();
    LOG.log(Level.INFO, "Test end: {0} in test no. {1}",
        new Object[]{test.toString(), testIndex});
    testNameToTimeList.add(Pair.of(test.toString(), endTime - startTime));

    executorSynchronizer.sync();
  }

  private void printResult() {
    for (final Pair<String, Long> testNameToTime : testNameToTimeList) {
      LOG.log(Level.INFO, "Time elapsed: {0} ms - in {1}",
          new Object[]{testNameToTime.getRight(), testNameToTime.getLeft()});
    }
  }

  private static String getExpectedValue(final long key,
                                         final boolean isUpdated,
                                         final boolean isNullTest) {
    if (isNullTest) {
      return null;
    }
    return isUpdated ? UPDATE_PREFIX + String.valueOf(key) : String.valueOf(key);
  }

  private Long getKeyByTestType(final int operationIdx) {

    switch (blockAccessType) {

    case RANDOM_ACCESS:
      return randomKeyArray[operationIdx];

    case ALL_BLOCKS_ACCESS:
      // the minimum key of block + key-offset
      return orderingBasedBlockPartitioner.getKeySpace(operationIdx).getLeft() + keyOffsetByExecutor;

    case ONE_BLOCK_ACCESS:
      final long rightIdx = orderingBasedBlockPartitioner.getKeySpace(BLOCK_ID_ACCESS).getRight();
      final long leftIdx = orderingBasedBlockPartitioner.getKeySpace(BLOCK_ID_ACCESS).getLeft();
      return (rightIdx - leftIdx) / NUM_OPERATIONS * operationIdx + keyOffsetByExecutor;

    default:
      throw new RuntimeException("There is wrong test type in this task.");
    }
  }

  private Long[] getRandomizedKeyArray() {

    int count = 0;
    final Set<Long> randomizedKeySet = new HashSet<>(NUM_OPERATIONS);
    while (count < NUM_OPERATIONS) {

      final long randomKey = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
      // If we perfectly randomized the key, it occur the duplication of key from other executors.
      // so interpolate keys by keyOffsetByExecutor.
      final long randomKeyByExecutor = randomKey - randomKey % numExecutorsToRunTask + keyOffsetByExecutor;
      if (!randomizedKeySet.contains(randomKeyByExecutor)) {
        randomizedKeySet.add(randomKeyByExecutor);
        count++;
      }
    }
    return randomizedKeySet.toArray(new Long[randomizedKeySet.size()]);
  }

  /**
   * Put method test class.
   */
  private final class PutTest implements Runnable {

    private final Table<Long, String, String> table;
    private final boolean isUpdated;
    private final boolean isNullTest;

    private PutTest(final Table<Long, String, String> table,
                    final boolean isUpdated,
                    final boolean isNullTest) {
      this.table = table;
      this.isUpdated = isUpdated;
      this.isNullTest = isNullTest;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < NUM_OPERATIONS; i++) {

          final long key = getKeyByTestType(i);
          final String value = String.valueOf(key);
          final String expectedValue = getExpectedValue(key, isUpdated, isNullTest);
          final String prevValue = table.put(key, value).get();

          if (isNullTest) {
            if (prevValue != null) {
              LOG.log(Level.SEVERE, "For key {2}, expected value is null, but the result value is {1}.",
                  new Object[]{prevValue, key});
              throw new RuntimeException("The result is different from the expectation");
            }
          } else if (prevValue == null || !prevValue.equals(expectedValue)) {
            LOG.log(Level.SEVERE, "For key {2}, expected value is {0}, but the result value is {1}.",
                new Object[]{expectedValue, prevValue, key});
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }

  /**
   * Multi-Put method test class.
   */
  private final class MultiPutTest implements Runnable {

    private final Table<Long, String, String> table;
    private final boolean isUpdated;
    private final boolean isNullTest;
    // it will call multiput method 10 times.

    private MultiPutTest(final Table<Long, String, String> table,
                         final boolean isUpdated,
                         final boolean isNullTest) {
      this.table = table;
      this.isUpdated = isUpdated;
      this.isNullTest = isNullTest;
    }

    @Override
    public void run() {
      try {
        // initialize the list which index and value are equal
        final List<Integer> indexList = new ArrayList<>(NUM_OPERATIONS);
        for (int index = 0; index < NUM_OPERATIONS; index++) {
          indexList.add(index);
        }

        // partition the index list to call multi put methods
        final int keySizePerOperation = NUM_OPERATIONS / NUM_MULTI_OPERATIONS;
        final List<List<Integer>> subIndexLists = Lists.partition(indexList, keySizePerOperation);
        for (final List<Integer> subIndexList : subIndexLists) {
          final List<Pair<Long, String>> kvPairList = new ArrayList<>(subIndexList.size());
          final Map<Long, String> expectedValueMap = new HashMap<>(subIndexList.size());

          for (final int index : subIndexList) {
            final long key = getKeyByTestType(index);
            final String value = String.valueOf(key);
            final String expectedValue = getExpectedValue(key, isUpdated, isNullTest);
            kvPairList.add(Pair.of(key, value));
            expectedValueMap.put(key, expectedValue);
          }

          final Map<Long, String> prevValueMap = table.multiPut(kvPairList).get();
          if (isNullTest) {
            for (final String prevValue : prevValueMap.values()) {
              if (prevValue != null) {
                LOG.log(Level.SEVERE, "Expected value {0}, Result value {1}",
                    new Object[]{null, prevValue});
                throw new RuntimeException("The result is different from the expectation");
              }
            }
          } else {
            prevValueMap.forEach((key, prevValue) -> {
              if (!expectedValueMap.get(key).equals(prevValue)) {
                LOG.log(Level.SEVERE, "ExpectedValueSet doesn't contain the Result value {0}", prevValue);
                throw new RuntimeException("The result is different from the expectation");
              }
            });
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }

  /**
   * Get method test class.
   */
  private final class GetTest implements Runnable {

    private final Table<Long, String, String> table;
    private final boolean isUpdated;
    private final boolean isNullTest;

    private GetTest(final Table<Long, String, String> table,
                    final boolean isUpdated,
                    final boolean isNullTest) {
      this.table = table;
      this.isUpdated = isUpdated;
      this.isNullTest = isNullTest;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < NUM_OPERATIONS; i++) {
          final long key = getKeyByTestType(i);
          final String expectedValue = getExpectedValue(key, isUpdated, isNullTest);
          final String value = table.get(key).get();

          // if data are removed, get method returns null
          if (isNullTest) {
            if (value != null) {
              throw new RuntimeException("The result is different from the expectation");
            }
            continue;
          }
          if (value == null || !value.equals(expectedValue)) {
            LOG.log(Level.SEVERE, "Expected value : {0}, result value : {1}",
                new Object[]{expectedValue, value});
            throw new RuntimeException("The result is different from the expectation");
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }

  /**
   * Update method test class.
   */
  private final class UpdateTest implements Runnable {

    private final Table<Long, String, String> table;

    private UpdateTest(final Table<Long, String, String> table) {
      this.table = table;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < NUM_OPERATIONS; i++) {
          final long key = getKeyByTestType(i);
          final String expectedValue = UPDATE_PREFIX + String.valueOf(key);
          final String updatedValue = table.update(key, UPDATE_PREFIX).get();

          if (!updatedValue.equals(expectedValue)) {
            LOG.log(Level.SEVERE, "Expected value : {0}, result value : {1}",
                new Object[]{expectedValue, updatedValue});
            throw new RuntimeException("The result is different from the expectation");
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }

  /**
   * Multi-Update method test class.
   */
  private final class MultiUpdateTest implements Runnable {

    private final Table<Long, String, String> table;
    private MultiUpdateTest(final Table<Long, String, String> table) {
      this.table = table;
    }

    @Override
    public void run() {
      try {
        // initialize the list which index and value are equal
        final List<Integer> indexList = new ArrayList<>(NUM_OPERATIONS);
        for (int index = 0; index < NUM_OPERATIONS; index++) {
          indexList.add(index);
        }

        // partition the index list to call multi update methods
        final int keySizePerOperation = NUM_OPERATIONS / NUM_MULTI_OPERATIONS;
        final List<List<Integer>> subIndexLists = Lists.partition(indexList, keySizePerOperation);
        for (final List<Integer> subIndexList : subIndexLists) {
          final List<Pair<Long, String>> kuPairList = new ArrayList<>(subIndexList.size());
          final Map<Long, String> expectedValueMap = new HashMap<>(subIndexList.size());

          for (final int index : subIndexList) {
            final long key = getKeyByTestType(index);
            final String expectedValue = getExpectedValue(key, true, false);
            kuPairList.add(Pair.of(key, UPDATE_PREFIX));
            expectedValueMap.put(key, expectedValue);
          }

          final Map<Long, String> updateValueMap = table.multiUpdate(kuPairList).get();
          updateValueMap.forEach((key, updatedValue) -> {
            if (!expectedValueMap.get(key).equals(updatedValue)) {
              LOG.log(Level.SEVERE, "ExpectedValueSet doesn't contain the Result value {0}", updatedValue);
              throw new RuntimeException("The result is different from the expectation");
            }
          });
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }

  /**
   * Remove method test class.
   */
  private final class RemoveTest implements Runnable {

    private final Table<Long, String, String> table;
    private final boolean isNullTest;
    private final boolean isUpdated;

    private RemoveTest(final Table<Long, String, String> table,
                       final boolean isUpdated,
                       final boolean isNullTest) {
      this.table = table;
      this.isUpdated = isUpdated;
      this.isNullTest = isNullTest;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < NUM_OPERATIONS; i++) {
          final long key = getKeyByTestType(i);
          final String expectedValue = getExpectedValue(key, isUpdated, isNullTest);
          final String prevValue = table.remove(key).get();

          if (isNullTest) {
            if (prevValue != null) {
              LOG.log(Level.SEVERE, "Expected value {0}, Result value {1} in key {2}",
                  new Object[]{null, prevValue, key});
              throw new RuntimeException("The result is different from the expectation");
            }
          } else if (prevValue == null || !prevValue.equals(expectedValue)) {
            LOG.log(Level.SEVERE, "Expected value : {0}, result value : {1}",
                new Object[]{expectedValue, prevValue});
            throw new RuntimeException("The result is different from the expectation");
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("There is unexpected exception in this method\n" + e);
      }
    }
  }
}
