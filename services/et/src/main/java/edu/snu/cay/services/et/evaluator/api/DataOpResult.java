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

package edu.snu.cay.services.et.evaluator.api;

import javax.annotation.Nullable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A class representing the result of table data operation.
 * @param <V> a type of data value
 */
public interface DataOpResult<V> extends Future<V> {

  /**
   * Commit the result of operation.
   * It releases a latch in {@link #get()} and {@link #get(long, TimeUnit)}.
   * @param result a result data
   * @param isSuccess a boolean that indicates whether the operation is succeeded or not
   */
  void onCompleted(@Nullable V result, boolean isSuccess);
}
