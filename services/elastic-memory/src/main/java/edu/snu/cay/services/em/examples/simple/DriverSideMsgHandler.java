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
package edu.snu.cay.services.em.examples.simple;

import edu.snu.cay.common.aggregation.avro.AggregationMessage;
import edu.snu.cay.common.aggregation.driver.AggregationMaster;
import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.avro.Result;
import edu.snu.cay.services.em.driver.api.ElasticMemory;
import edu.snu.cay.services.em.driver.impl.PartitionManager;
import edu.snu.cay.services.em.examples.simple.parameters.NumMoves;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.htrace.Sampler;
import org.htrace.Trace;
import org.htrace.TraceScope;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver-side message handler that receives aggregation messages as an aggregation master.
 * In default, it synchronizes all tasks by checking all evaluators have sent the messages.
 * To make this happen, it sends response messages to all evaluators when messages from all evaluatorss arrive.
 * Also it runs Move between two evaluators randomly chosen when all evaluators say they are READY
 * and sends the result to all evaluators.
 */
public final class DriverSideMsgHandler implements EventHandler<AggregationMessage> {
  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());
  private static final Random RANDOM = new Random();

  static final String READY = "READY";

  private final ElasticMemory elasticMemory;
  private final PartitionManager partitionManager;

  private final AggregationMaster aggregationMaster;
  private final Codec<String> codec;
  private CountDownLatch msgCountDown = new CountDownLatch(SimpleEMDriver.NUM_EVAL);
  private final List<String> evalIds = new ArrayList<>(SimpleEMDriver.NUM_EVAL);

  private final int numMoves;

  @Inject
  private DriverSideMsgHandler(final AggregationMaster aggregationMaster,
                               final ElasticMemory elasticMemory,
                               final PartitionManager partitionManager,
                               final SerializableCodec<String> codec,
                               @Parameter(NumMoves.class) final int numMoves) {
    this.aggregationMaster = aggregationMaster;
    this.elasticMemory = elasticMemory;
    this.partitionManager = partitionManager;
    this.codec = codec;
    this.numMoves = numMoves;
    syncWorkers();
  }

  /**
   * Aggregation message handling logic.
   *
   * @param message received aggregation message
   * @throws RuntimeException if the received message is incorrect
   */
  @Override
  public void onNext(final AggregationMessage message) {
    // let's assume that the message is always the READY msg

    LOG.log(Level.INFO, "Received message {0} from eval", message);
    final String workerId = message.getSourceId().toString();

    if (!evalIds.contains(workerId)) {
      evalIds.add(workerId);
    }

    msgCountDown.countDown();
  }

  /**
   * Start synchronizing workers by executing a thread controlling workers.
   */
  private void syncWorkers() {
    LOG.log(Level.INFO, "Start synchronization of evaluators...");
    final Thread syncThread = new Thread(new SyncThread());
    syncThread.start();
  }

  /**
   * A Runnable that runs Move, when all evaluators sent READY messages to driver.
   * Workers can make a progress when driver sends a message.
   */
  private class SyncThread implements Runnable {

    @Override
    public void run() {
      for (int i = 0; i < numMoves; i++) {

        LOG.log(Level.INFO, "Waiting for workers ready for {0}th move.", i + 1);
        // wait until all workers are sent msg
        try {
          msgCountDown.await();
          // reset for next iteration
          msgCountDown = new CountDownLatch(SimpleEMDriver.NUM_EVAL);
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }

        // Choose two evaluators and move data between them
        while (true) {
          // choose a source evaluator that has some blocks
          final int srcEvalIdx = RANDOM.nextInt(SimpleEMDriver.NUM_EVAL);
          final String srcEvalId = evalIds.get(srcEvalIdx);
          final int initialSrcNumBlocks = getNumBlocks(srcEvalId);
          if (initialSrcNumBlocks == 0) {
            continue;
          }

          // choose a destination evaluator that is different from the source
          int anotherRandomIdx;
          do {
            anotherRandomIdx = RANDOM.nextInt(SimpleEMDriver.NUM_EVAL);
          } while (anotherRandomIdx == srcEvalIdx);
          final int destEvalIdx = anotherRandomIdx;
          final String destEvalId = evalIds.get(destEvalIdx);

          final int movedBlocks = runMove(srcEvalId, destEvalId);

          // tell workers that move is finished
          sendResultToEvals(srcEvalId, destEvalId, movedBlocks);
          break;
        }
      }

      // wait one more time for last sync
      try {
        msgCountDown.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      sendResponseToEvals();
    }

    /**
     * Moves data from a src evaluator to a dest evaluator.
     */
    private int runMove(final String srcId, final String destId) {
      final CountDownLatch finishedLatch = new CountDownLatch(1);

      final int initialSrcNumBlocks = getNumBlocks(srcId);
      final int initialDestNumBlocks = getNumBlocks(destId);
      final int numToMove = Math.max(1, RANDOM.nextInt(initialSrcNumBlocks)); // Move at least one block
      LOG.log(Level.INFO, "Move {0} blocks from {1} to {2} (Initial number of blocks: {3} / {4} respectively)",
          new Object[]{numToMove, srcId, destId, initialSrcNumBlocks, initialDestNumBlocks});

      final boolean[] moveSucceeded = {false};

      // start move
      try (final TraceScope moveTraceScope = Trace.startSpan("simpleMove", Sampler.ALWAYS)) {
        elasticMemory.move(SimpleEMTask.DATATYPE, numToMove, srcId, destId,
            new EventHandler<AvroElasticMemoryMessage>() {
              @Override
              public void onNext(final AvroElasticMemoryMessage emMsg) {
                moveSucceeded[0] = emMsg.getResultMsg().getResult().equals(Result.SUCCESS);
                LOG.log(Level.INFO, "Was Move {0} successful? {1}. The result: {2}",
                    new Object[]{emMsg.getOperationId(), moveSucceeded[0],
                        emMsg.getResultMsg() == null ? "" : emMsg.getResultMsg().getResult()});
                finishedLatch.countDown();
              }
            }
        );
      }

      // Wait for move to succeed
      try {
        LOG.log(Level.INFO, "Waiting for move to finish");
        finishedLatch.await();

        // After moved, number of blocks should be updated accordingly.
        checkNumBlocks(srcId, initialSrcNumBlocks - numToMove);
        checkNumBlocks(destId, initialDestNumBlocks + numToMove);

        if (moveSucceeded[0]) {
          LOG.log(Level.INFO, "Move finished successfully");
        } else {
          throw new RuntimeException("Move failed");
        }
      } catch (final InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting Move", e);
      }

      return numToMove;
    }

    /**
     * Tells the result of move to evals and makes them continue.
     */
    private void sendResultToEvals(final String srcEvalId, final String destEvalId, final int numBlocksMoved) {
      for (final String evalId : evalIds) {
        final int numChangedBlocks;
        if (evalId.equals(srcEvalId)) {
          numChangedBlocks = -numBlocksMoved;
        } else if (evalId.equals(destEvalId)) {
          numChangedBlocks = numBlocksMoved;
        } else {
          numChangedBlocks = 0;
        }

        LOG.log(Level.INFO, "Sending the result ({0}) to {1}", new Object[]{numChangedBlocks, evalId});
        aggregationMaster.send(SimpleEMDriver.AGGREGATION_CLIENT_ID, evalId,
            codec.encode(Long.toString(numChangedBlocks)));
      }
    }

    /**
     * Tells the evals to continue their progress.
     */
    private void sendResponseToEvals() {
      LOG.log(Level.INFO, "Sending response to all evals: {0}", evalIds);
      for (final String evalId : evalIds) {
        aggregationMaster.send(SimpleEMDriver.AGGREGATION_CLIENT_ID, evalId, codec.encode(Long.toString(0)));
      }
    }

    /**
     * Checks the expected number of blocks are in the Evaluator.
     */
    private void checkNumBlocks(final String evalId, final int expected) {
      final int actual = getNumBlocks(evalId);
      if (actual != expected) {
        final String msg = evalId + "should have " + expected + ", but has " + actual + " blocks";
        throw new RuntimeException(msg);
      }
    }

    /**
     * Gets the number of blocks in the Evaluator.
     */
    private int getNumBlocks(final String evalId) {
      return partitionManager.getNumBlocks(evalId);
    }
  }
}
