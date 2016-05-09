/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.services.ps.server.partitioned;

/**
 * A class containing key with its hash value.
 * @param <K> a type of key
 */
public class HashedKey<K> {
  private K key;
  private final int hash;

  public HashedKey(final K key, final int hash) {
    this.key = key;
    this.hash = hash;
  }

  public K getKey() {
    return key;
  }

  public int getHash() {
    return hash;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HashedKey<?> that = (HashedKey<?>) o;

    return key.equals(that.key);
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
