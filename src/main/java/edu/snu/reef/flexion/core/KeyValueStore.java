package edu.snu.reef.flexion.core;


import javax.inject.Inject;
import java.util.HashMap;

/**
 * Simple Key-value store used by key-value store service
 */
public final class KeyValueStore {

  private final HashMap<Class<? extends Key>, Object> hashMap;

  @Inject
  public KeyValueStore() {
    hashMap = new HashMap<>();
  }

  public <T> void put(Class<? extends Key<T>> key, T value) {
    hashMap.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(Class<? extends Key<T>> key) {
    return (T) hashMap.get(key);
  }
}
