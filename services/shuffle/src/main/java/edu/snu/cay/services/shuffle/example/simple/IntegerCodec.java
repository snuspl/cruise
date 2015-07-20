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
package edu.snu.cay.services.shuffle.example.simple;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * Simple integer codec
 */
public class IntegerCodec implements Codec<Integer> {

  @Inject
  public IntegerCodec() {
  }

  @Override
  public Integer decode(final byte[] buf) {
    return Integer.decode(new String(buf));
  }

  @Override
  public byte[] encode(final Integer obj) {
    return Integer.toString(obj).getBytes();
  }
}
