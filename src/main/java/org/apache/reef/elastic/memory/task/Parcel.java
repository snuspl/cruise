package org.apache.reef.elastic.memory.task;

import org.apache.reef.io.network.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class Parcel<K,V> {

  private final List<Pair<K, V>> recordsList;

  Parcel(List<Pair<K,V>> list) {
    this.recordsList = list;
  }
}
