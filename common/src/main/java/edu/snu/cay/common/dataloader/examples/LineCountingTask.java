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
package edu.snu.cay.common.dataloader.examples;

import edu.snu.cay.common.dataloader.HdfsDataSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.DriverMessage;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The task that loads a file given through {@link DriverMessage} and counts the number of records.
 * Task will finish after loading all files specified by clients.
 */
@TaskSide
//@Unit
final class LineCountingTask implements Task {
  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());

  private final AtomicReference<byte[]> msgToSend = new AtomicReference<>(null);

  private final CountDownLatch finishedLatch = new CountDownLatch(1);


  @Inject
  private LineCountingTask() {
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    // wait until CloseEvent has been received. See {@link CloseEventHandler}.
    //finishedLatch.await();
    //final byte[] msg = msgToSend.getAndSet(null);
    final byte[] msg = bytes;
    final HdfsDataSet<LongWritable, Text> dataSet;
    try {
      dataSet = HdfsDataSet.from(bytes);
    } catch (final IOException e) {
      throw new RuntimeException("Exception while instantiating a HdfsDataSet", e);
    }

    LOG.log(Level.FINER, "LineCounting task started");

    int count = 0;
    for (final Pair<LongWritable, Text> recordPair : dataSet) {
      LOG.log(Level.FINEST, "Read line: {0}", recordPair);
      count++;
    }
    LOG.log(Level.INFO, "LineCounting task finished: read {0} lines", count);
    //msgToSend.set(Integer.toString(count).getBytes(StandardCharsets.UTF_8));
    return Integer.toString(count).getBytes(StandardCharsets.UTF_8);
  }
}
