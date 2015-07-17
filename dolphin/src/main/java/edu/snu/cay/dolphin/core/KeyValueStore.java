/**
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.core;


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
