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
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitInfoSerializer;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import java.io.IOException;
import java.util.Iterator;

/**
 * An implementation of {@link DataSet} that loads data from a serialized {@link HdfsSplitInfo},
 * using {@link HdfsSplitInfoSerializer#deserialize(String)} and {@link HdfsSplitFetcher#fetchData(HdfsSplitInfo)}.
 * @param <K> a type of key
 * @param <V> a type of value
 */
public final class RawDataSet<K, V> implements DataSet<K, V> {
  private static final HdfsSplitInfoSerializer.HdfsSplitInfoCodec CODEC =
      new HdfsSplitInfoSerializer.HdfsSplitInfoCodec();

  private final Iterator<Pair<K, V>> recordIter;

  private RawDataSet(final HdfsSplitInfo hdfsSplitInfo) throws IOException {
    this.recordIter = HdfsSplitFetcher.fetchData(hdfsSplitInfo);
  }

  /**
   * Instantiates a RawDataSet from a serialized HdfsSplitInfo.
   * @param serializedHdfsSplitInfo a string form of serialized HdfsSplitInfo
   * @return RawDataSet
   * @throws IOException
   */
  public static RawDataSet from(final String serializedHdfsSplitInfo) throws IOException {
    final HdfsSplitInfo hdfsSplitInfo = HdfsSplitInfoSerializer.deserialize(serializedHdfsSplitInfo);
    return new RawDataSet<>(hdfsSplitInfo);
  }

  /**
   * Instantiates a RawDataSet from a serialized HdfsSplitInfo.
   * @param serializedHdfsSplitInfo a byte array form of serialized HdfsSplitInfo
   * @return RawDataSet
   * @throws IOException
   */
  public static RawDataSet from(final byte[] serializedHdfsSplitInfo) throws IOException {
    final HdfsSplitInfo hdfsSplitInfo = CODEC.decode(serializedHdfsSplitInfo);
    return new RawDataSet(hdfsSplitInfo);
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    return recordIter;
  }
}
