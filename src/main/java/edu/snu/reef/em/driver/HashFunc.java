package edu.snu.reef.em.driver;

import java.util.List;

public interface HashFunc<K> {
  public int hash(K key);
}
