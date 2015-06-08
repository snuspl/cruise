package edu.snu.reef.elastic.memory.driver;

import java.util.List;

public interface HashFunc<K> {
  public int hash(K key);
}
