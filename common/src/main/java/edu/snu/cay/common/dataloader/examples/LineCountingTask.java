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

import edu.snu.cay.common.dataloader.HdfsSplitFetcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The task that loads file through {@link HdfsSplitFetcher} and counts the number of records.
 * Assumes TextInputFormat and each record represents a line.
 */
@TaskSide
final class LineCountingTask implements Task {
  private static final Logger LOG = Logger.getLogger(LineCountingTask.class.getName());

  private final DataSet<LongWritable, Text> dataSet;

  @Inject
  private LineCountingTask(@Parameter(LineCountingDriver.SerializedSplitInfo.class) final String serializedSplitInfo)
      throws IOException {

    dataSet = new RawDataSet<>(serializedSplitInfo);
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.FINER, "LineCounting task started");

    int count = 0;

    for (final Pair<LongWritable, Text> recordPair : dataSet) {
      LOG.log(Level.FINEST, "Read line: {0}", recordPair);
      count++;
    }

    LOG.log(Level.FINER, "LineCounting task finished: read {0} lines", count);
    return Integer.toString(count).getBytes(StandardCharsets.UTF_8);
  }
}
