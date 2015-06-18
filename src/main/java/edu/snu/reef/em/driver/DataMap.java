package edu.snu.reef.em.driver;

import java.util.Map;

public class DataMap<K> {

  Map<String, Map<K, String>> typeToKeyToEval;

  public void remove(String dataType) {

  }

  public void add() {

  }

  public void move(String dataType, K key, String destEvalId) {
    Map<K, String> keyToEval = typeToKeyToEval.get(dataType);
    String srcEvalId = keyToEval.put(key, destEvalId);
  }

}
