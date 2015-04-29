package org.apache.reef.elastic.memory.task;

import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Raw form of data in each evaluator
 */
public final class ParcelableDataSet<K extends Comparable<K>, V>
    implements DataSet<K, V> {

  private final List<Pair<K, V>> recordsList = new ArrayList<>();

  public ParcelableDataSet() {
  }

  public ParcelableDataSet(DataSet<K, V> dataSet) {
    addData(dataSet);
  }

  @Override
  public synchronized Iterator<Pair<K, V>> iterator() {
    return recordsList.iterator();
  }

  public List<Pair<K,V>> getDataList() {
    return recordsList;
  }

  public Parcel getParcel(float ratio) {
    int size = (int)(recordsList.size() * ratio);
    if(size > 0)
      return new Parcel(recordsList.subList(0, size));
    else return null;
  }

  public Parcel getParcel(Set<K> keySet) {
    throw new UnsupportedOperationException("");
  }

  public Parcel getParcel(int fromIndex, int toIndex) {
    return new Parcel(recordsList.subList(fromIndex, toIndex));
  }

  public void addData(DataSet<K,V> dataSet) {
    if(dataSet != null) {
      for (final Pair<K, V> keyValue : dataSet) {
        recordsList.add(keyValue);
      }
    }
  }

  public void mergeDataSet(ParcelableDataSet<K,V> data) {
    recordsList.addAll(data.getDataList());
  }

  public void removeDataSet(ParcelableDataSet<K,V> data) {
    recordsList.removeAll(data.getDataList());
  }

  @Override
  public String toString() {
    return recordsList.toString();
  }
}