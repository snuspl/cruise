/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.spl.cruise.services.et.evaluator.impl;

import edu.snu.spl.cruise.services.et.configuration.parameters.KeyCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.spl.cruise.services.et.configuration.parameters.ValueCodec;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A serializer that provides codecs for both key and value of a table.
 * @param <K> a key type in table
 * @param <V> a value type in table
 */
final class KVUSerializer<K, V, U> {

  private final StreamingCodec<K> keyCodec;
  private final StreamingCodec<V> valueCodec;
  private final Codec<U> updateValueCodec;

  @Inject
  private KVUSerializer(@Parameter(KeyCodec.class) final StreamingCodec<K> keyCodec,
                        @Parameter(ValueCodec.class) final StreamingCodec<V> valueCodec,
                        @Parameter(UpdateValueCodec.class) final Codec<U> updateValueCodec) {
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
    this.updateValueCodec = updateValueCodec;
  }

  /**
   * @return a key codec
   */
  StreamingCodec<K> getKeyCodec() {
    return keyCodec;
  }

  /**
   * @return a value codec
   */
  StreamingCodec<V> getValueCodec() {
    return valueCodec;
  }

  /**
   * @return an update value codec
   */
  Codec<U> getUpdateValueCodec() {
    return updateValueCodec;
  }
}
