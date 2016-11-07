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
package edu.snu.cay.services.ps.worker.impl;

import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.reef.io.serialization.Codec;

final class EncodedKey<K> {
  private final K key;
  private final int hash;
  private final byte[] encoded;

  EncodedKey(final K key, final Codec<K> keyCodec) {
    this.key = key;
    this.encoded = keyCodec.encode(key);
    this.hash = computeHash();
  }

  public K getKey() {
    return key;
  }

  public int getHash() {
    return hash;
  }

  public byte[] getEncoded() {
    return encoded;
  }

  private int computeHash() {
    return Math.abs(MurmurHash.getInstance().hash(encoded));
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final EncodedKey<?> that = (EncodedKey<?>) o;

    return key.equals(that.key);
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
