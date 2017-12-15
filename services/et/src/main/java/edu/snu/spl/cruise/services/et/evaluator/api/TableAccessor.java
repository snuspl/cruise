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
package edu.snu.spl.cruise.services.et.evaluator.api;

import edu.snu.spl.cruise.services.et.evaluator.impl.Tables;
import edu.snu.spl.cruise.services.et.exceptions.TableNotExistException;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * An interface for tasks to access tables by specifying id.
 */
@DefaultImplementation(Tables.class)
@EvaluatorSide
public interface TableAccessor {

  /**
   * Gets a table that its id is {@code tableId}.
   * @param tableId a table Id
   * @return a table whose id is tableId
   * @throws TableNotExistException when there's no table with the specified id
   */
  <K, V, U> Table<K, V, U> getTable(String tableId) throws TableNotExistException;
}
