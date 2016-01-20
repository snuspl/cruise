/*
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
package edu.snu.cay.services.ps.server.concurrent.impl;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wrapper class for parameter values, for avoiding {@link NullPointerException}s and
 * creating a {@link ReadWriteLock} for each value.
 * @param <V> class type of parameter values after they are processed at the server
 */
public final class ValueEntry<V> {
  private final ReadWriteLock readWriteLock;
  private V value;

  public ValueEntry() {
    this(null);
  }

  public ValueEntry(final V value) {
    this.readWriteLock = new ReentrantReadWriteLock();
    this.value = value;
  }

  public ReadWriteLock getReadWriteLock() {
    return this.readWriteLock;
  }

  public V getValue() {
    return this.value;
  }

  public void setValue(final V value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return "ValueEntry[" + (value == null ? "NULL" : value.toString()) + "]";
  }
}
