package org.apache.reef.elastic.memory.task;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Raw form of data in each evaluator
 */
public final class ElasticDataSet<K extends WritableComparable<K>, V extends Writable>
    implements DataSet<K, V> {

  private final List<Pair<K, V>> recordsList = new ArrayList<>();

  public ElasticDataSet() {
  }

  public ElasticDataSet(DataSet<K, V> dataSet) {
    addData(dataSet);
  }

  @Override
  public synchronized Iterator<Pair<K, V>> iterator() {
    return recordsList.iterator();
  }

  public List<Pair<K,V>> getDataList() {
    return recordsList;
  }

  public void addData(DataSet<K,V> dataSet) {
    if(dataSet != null) {
      for (final Pair<K, V> keyValue : dataSet) {
        recordsList.add(keyValue);
      }
    }
  }

  public void mergeDataSet(ElasticDataSet<K,V> data) {
    recordsList.addAll(data.getDataList());
  }

  public void removeDataSet(ElasticDataSet<K,V> data) {
    recordsList.removeAll(data.getDataList());
  }

  @Override
  public String toString() {
    return recordsList.toString();
  }
}