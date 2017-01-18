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
package edu.snu.cay.common.dataloader;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.ExternalConstructor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used in Evaluator to fetch a split data from HDFS.
 */
public final class HdfsSplitFetcher {

  private static final Reporter DUMMY_REPORTER = new DummyReporter();

  // utility class should not be instantiated
  private HdfsSplitFetcher() {

  }

  /**
   * @param hdfsSplitInfo information of a split
   * @param <K> type of the split
   * @param <V> type of the split
   * @return the split data
   */
  public static <K, V> List<Pair<K, V>> fetchData(final HdfsSplitInfo hdfsSplitInfo) throws IOException {
    final ExternalConstructor<JobConf> jobConfExternalConstructor =
        new JobConfExternalConstructor(hdfsSplitInfo.getInputFormatClassName(), hdfsSplitInfo.getInputPath());

    final JobConf jobConf = jobConfExternalConstructor.newInstance();

    final RecordReader<K, V> newRecordReader = jobConf.getInputFormat().getRecordReader(
        hdfsSplitInfo.getInputSplit(), jobConfExternalConstructor.newInstance(), DUMMY_REPORTER);

    // fetch records into a list
    final List<Pair<K, V>> recordList = new ArrayList<>();
    while (true) {
      final Pair<K, V> record = new Pair<>(newRecordReader.createKey(), newRecordReader.createValue());
      if (newRecordReader.next(record.getFirst(), record.getSecond())) {
        recordList.add(record);
      } else {
        break;
      }
    }

    return recordList;
  }

  private static final class DummyReporter implements Reporter {

    @Override
    public void progress() {
    }

    @Override
    public Counter getCounter(final Enum<?> key) {
      return null;
    }

    @Override
    public Counter getCounter(final String group, final String name) {
      return null;
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      throw new UnsupportedOperationException("This is a Fake Reporter");
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void incrCounter(final Enum<?> key, final long amount) {
    }

    @Override
    public void incrCounter(final String group, final String counter, final long amount) {
    }

    @Override
    public void setStatus(final String status) {
    }
  }
}
