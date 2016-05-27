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
package edu.snu.cay.async;

import  edu.snu.cay.common.param.Parameters.Iterations;
import edu.snu.cay.common.param.Parameters.NumWorkerThreads;
import edu.snu.cay.services.em.evaluator.api.DataIdFactory;
import edu.snu.cay.services.em.evaluator.impl.OperationRouter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.driver.task.TaskConfigurationOptions.Identifier;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REEF Task for worker threads of {@code dolphin-async} applications.
 * Spawns several computation threads and runs them using
 * a fixed thread pool ({@link Executors#newFixedThreadPool(int)}.
 */
@Unit
final class AsyncWorkerTask implements Task {
  private static final Logger LOG = Logger.getLogger(AsyncWorkerTask.class.getName());
  static final String TASK_ID_PREFIX = "AsyncWorkerTask";

  private final String taskId;
  private final int maxIterations;
  private final int numWorkerThreads;
  private final Injector injector;
  private final DataSet<LongWritable, Text> dataSet;

  /**
   * A boolean flag shared among all worker threads.
   * Worker threads end when this flag becomes true by {@link CloseEventHandler#onNext(CloseEvent)}.
   */
  private volatile boolean aborted = false;

  @Inject
  private AsyncWorkerTask(@Parameter(Identifier.class) final String taskId,
                          @Parameter(Iterations.class) final int maxIterations,
                          @Parameter(NumWorkerThreads.class) final int numWorkerThreads,
                          final Injector injector,
                          final DataIdFactory<Long> idFactory,
                          final OperationRouter<Long> router,
                          final DataSet<LongWritable, Text> dataSet) {
    // inject DataIdFactory and OperationRouter here to make spawning threads share the same instances
    this.taskId = taskId;
    this.maxIterations = maxIterations;
    this.numWorkerThreads = numWorkerThreads;
    this.injector = injector;
    this.dataSet = dataSet;
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "{0} starting...", taskId);

    final AsyncWorkerDataSet[] asyncWorkerDataSets = divideDataSets();
    final ExecutorService executorService = Executors.newFixedThreadPool(numWorkerThreads);
    final Future[] futures = new Future[numWorkerThreads];

    LOG.log(Level.INFO, "Initializing {0} worker threads", numWorkerThreads);
    for (int index = 0; index < numWorkerThreads; index++) {
      // since a single injector only gives singleton instances,
      // we need to fork the given injector every time we spawn a new thread
      final Injector forkedInjector = injector.forkInjector();
      forkedInjector.bindVolatileInstance(DataSet.class, asyncWorkerDataSets[index]);

      final Worker worker = forkedInjector.getInstance(Worker.class);
      futures[index] = executorService.submit(new Runnable() {
        @Override
        public void run() {
          worker.initialize();
          for (int iteration = 0; iteration < maxIterations; ++iteration) {
            if (aborted) {
              LOG.log(Level.INFO, "Abort a thread to completely close the task");
              return;
            }
            worker.run();
          }
          worker.cleanup();
        }
      });
    }

    // wait until all threads terminate
    for (int index = 0; index < numWorkerThreads; index++) {
      futures[index].get();
    }

    return null;
  }

  /**
   * Split the given dataset across worker computation threads, round-robin fashion.
   */
  private AsyncWorkerDataSet[] divideDataSets() {
    final AsyncWorkerDataSet[] asyncWorkerDataSets = new AsyncWorkerDataSet[numWorkerThreads];
    for (int index = 0; index < numWorkerThreads; index++) {
      asyncWorkerDataSets[index] = new AsyncWorkerDataSet();
    }

    int dataSetIndex = 0;
    for (final Pair<LongWritable, Text> pair : dataSet) {
      asyncWorkerDataSets[dataSetIndex].addPair(pair);
      dataSetIndex = (dataSetIndex + 1) % numWorkerThreads;
    }

    return asyncWorkerDataSets;
  }

  /**
   * {@link DataSet} implementation for worker computation threads.
   * Internally stores a list of data pairs that are exposed to threads as an {@link Iterator}.
   */
  private final class AsyncWorkerDataSet implements DataSet<LongWritable, Text> {

    private final List<Pair<LongWritable, Text>> dataSet;

    AsyncWorkerDataSet() {
      dataSet = new LinkedList<>();
    }

    void addPair(final Pair<LongWritable, Text> pair) {
      dataSet.add(pair);
    }

    @Override
    public Iterator<Pair<LongWritable, Text>> iterator() {
      return dataSet.iterator();
    }
  }

  final class CloseEventHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      aborted = true;
    }
  }
}
