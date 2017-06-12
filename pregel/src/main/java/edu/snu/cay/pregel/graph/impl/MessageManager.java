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
package edu.snu.cay.pregel.graph.impl;

import edu.snu.cay.pregel.PregelDriver;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.evaluator.api.TableAccessor;
import edu.snu.cay.services.et.exceptions.TableNotExistException;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Logger;

/**
 * Manage message stores which are used to computation in one superstep.
 * Determine the incoming message store depending on the state of a worker.
 *
 * @param <Long> identifier of the vertex
 * @param <M> message type of the vertex
 */
public final class MessageManager<Long, M> {

  private static final Logger LOG = Logger.getLogger(MessageManager.class.getName());

  private Table<Long, List<M>, M> messageTable1;

  private Table<Long, List<M>, M> messageTable2;

  private boolean tableFlag;

  @Inject
  private MessageManager(final TableAccessor tableAccessor) throws TableNotExistException {
    messageTable1 = tableAccessor.getTable(PregelDriver.MSG_TABLE_1_ID);
    messageTable2 = tableAccessor.getTable(PregelDriver.MSG_TABLE_2_ID);
    tableFlag = true;
  }

  /**
   * It switches current message table and next message table.
   */
  public void prepareForNextSuperstep() {
    tableFlag = !tableFlag;
  }

  public Table<Long, List<M>, M> getCurrentMessageTable() {
    return tableFlag ? messageTable1 : messageTable2;
  }

  public Table<Long, List<M>, M> getNextMessageTable() {
    return tableFlag ? messageTable2 : messageTable1;
  }
}

