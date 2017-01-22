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

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Iterator;

/**
 * A view of the data set to be loaded at an evaluator as an iterable of key value pairs.
 * It loads data in HDFS from a serialized {@link HdfsSplitInfo},
 * using {@link HdfsSplitInfoSerializer#deserialize(String)} and {@link HdfsSplitFetcher#fetchData(HdfsSplitInfo)}.
 * @param <K> a type of key
 * @param <V> a type of value
 */
public final class HdfsDataSet<K, V> implements Iterable<Pair<K, V>> {
  private static final HdfsSplitInfoSerializer.HdfsSplitInfoCodec CODEC =
      new HdfsSplitInfoSerializer.HdfsSplitInfoCodec();

  private final Iterator<Pair<K, V>> recordIter;

  private HdfsDataSet(final HdfsSplitInfo hdfsSplitInfo) throws IOException {
    this.recordIter = HdfsSplitFetcher.fetchData(hdfsSplitInfo);
  }

  /**
   * Instantiates a HdfsDataSet from a serialized HdfsSplitInfo.
   * @param serializedHdfsSplitInfo a string form of serialized HdfsSplitInfo
   * @return HdfsDataSet
   * @throws IOException
   */
  public static HdfsDataSet from(final String serializedHdfsSplitInfo) throws IOException {
    final HdfsSplitInfo hdfsSplitInfo = HdfsSplitInfoSerializer.deserialize(serializedHdfsSplitInfo);
    return new HdfsDataSet<>(hdfsSplitInfo);
  }

  /**
   * Instantiates a HdfsDataSet from a serialized HdfsSplitInfo.
   * @param serializedHdfsSplitInfo a byte array form of serialized HdfsSplitInfo
   * @return HdfsDataSet
   * @throws IOException
   */
  public static HdfsDataSet from(final byte[] serializedHdfsSplitInfo) throws IOException {
    final HdfsSplitInfo hdfsSplitInfo = CODEC.decode(serializedHdfsSplitInfo);
    return new HdfsDataSet(hdfsSplitInfo);
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    return recordIter;
  }
}
