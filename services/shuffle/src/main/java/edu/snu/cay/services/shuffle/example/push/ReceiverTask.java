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
package edu.snu.cay.services.shuffle.example.push;

import edu.snu.cay.services.shuffle.evaluator.Shuffle;
import edu.snu.cay.services.shuffle.evaluator.ShuffleProvider;
import edu.snu.cay.services.shuffle.evaluator.operator.PushShuffleReceiver;
import org.apache.reef.io.Tuple;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public final class ReceiverTask implements Task {

  private static final Logger LOG = Logger.getLogger(SenderTask.class.getName());

  private final PushShuffleReceiver<Integer, Integer> shuffleReceiver;

  @Inject
  private ReceiverTask(final ShuffleProvider shuffleProvider) {
    final Shuffle<Integer, Integer> shuffle = shuffleProvider
        .getShuffle(MessageExchangeDriver.MESSAGE_EXCHANGE_SHUFFLE_NAME);
    this.shuffleReceiver = shuffle.getReceiver();
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    int receivedTupleCount = 0;
    for (int i = 0; i < MessageExchangeDriver.ITERATION_NUMBER; i++) {
      LOG.log(Level.INFO, "Receive tuples from senders");
      for (final Tuple<Integer, Integer> tuple : shuffleReceiver.receive()) {
        LOG.log(Level.INFO, "A tuple arrived {0}", tuple);
        receivedTupleCount++;
      }

      LOG.log(Level.INFO, "Finish iteration " + i);
    }

    final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(receivedTupleCount);
    return byteBuffer.array();
  }
}
