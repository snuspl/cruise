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
  private final Iterator<Pair<K, V>> recordIter;

  public RawDataSet(final String serializedHdfsSplitInfo) throws IOException {
    final HdfsSplitInfo hdfsSplitInfo = HdfsSplitInfoSerializer.deserialize(serializedHdfsSplitInfo);
    this.recordIter = HdfsSplitFetcher.fetchData(hdfsSplitInfo);
  }

  @Override
  public Iterator<Pair<K, V>> iterator() {
    return recordIter;
  }
}
