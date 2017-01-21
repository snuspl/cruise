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

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.network.util.Pair;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.Iterator;

/**
 * Used in Evaluator to fetch a split data from HDFS.
 */
@EvaluatorSide
public final class HdfsSplitFetcher {
  private static final Reporter DUMMY_REPORTER = new DummyReporter();

  // utility class should not be instantiated
  private HdfsSplitFetcher() {
  }

  /**
   * @param hdfsSplitInfo information of a split
   * @param <K> a type of key
   * @param <V> a type of value
   * @return an iterator of split
   */
  public static <K, V> Iterator<Pair<K, V>> fetchData(final HdfsSplitInfo hdfsSplitInfo) throws IOException {
    final JobConf jobConf;
    try {
      final Tang tang = Tang.Factory.getTang();
      jobConf = tang.newInjector(tang.newConfigurationBuilder()
              .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class,
                  hdfsSplitInfo.getInputFormatClassName())
              .bindNamedParameter(JobConfExternalConstructor.InputPath.class, hdfsSplitInfo.getInputPath())
              .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
              .build())
          .getInstance(JobConf.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Exception while injecting JobConf", e);
    }

    final InputFormat<K, V> inputFormat = jobConf.getInputFormat();
    final RecordReader<K, V> recordReader = inputFormat.getRecordReader(
        hdfsSplitInfo.getInputSplit(), jobConf, DUMMY_REPORTER);

    final class RecordReaderIterator implements Iterator<Pair<K, V>> {
      private final RecordReader<K, V> recordReader;
      private Pair<K, V> recordPair;
      private boolean hasNext;

      private RecordReaderIterator(final RecordReader<K, V> recordReader) {
        this.recordReader = recordReader;
        fetchRecord();
      }

      @Override
      public boolean hasNext() {
        return this.hasNext;
      }

      @Override
      public Pair<K, V> next() {
        final Pair<K, V> prevRecordPair = this.recordPair;
        fetchRecord();
        return prevRecordPair;
      }

      private void fetchRecord() {
        this.recordPair = new Pair<>(this.recordReader.createKey(), this.recordReader.createValue());
        try {
          this.hasNext = this.recordReader.next(this.recordPair.getFirst(), this.recordPair.getSecond());
        } catch (final IOException ex) {
          throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", ex);
        }
      }
    }

    return new RecordReaderIterator(recordReader);
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
