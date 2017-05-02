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
package edu.snu.cay.services.et.plan.impl;

import edu.snu.cay.services.et.driver.api.AllocatedExecutor;
import edu.snu.cay.services.et.driver.impl.AllocatedTable;
import edu.snu.cay.services.et.driver.impl.MigrationResult;
import edu.snu.cay.services.et.driver.impl.SubmittedTask;
import edu.snu.cay.services.et.plan.api.Op;
import edu.snu.cay.services.et.plan.api.Op.OpType;
import edu.snu.cay.services.et.plan.impl.op.*;

/**
 * An abstract class of operation result.
 * Results for each type of operations extends this class.
 */
public abstract class OpResult {
  private final OpType type;
  private final Op op;

  OpResult(final OpType type, final Op op) {
    this.type = type;
    this.op = op;
  }

  /**
   * @return a type of operation
   */
  public OpType getType() {
    return type;
  }

  /**
   * @return an {@link Op}
   */
  public Op getOp() {
    return op;
  }

  public static final class AllocateOpResult extends OpResult {
    private final AllocatedExecutor executor;

    public AllocateOpResult(final AllocateOp op, final AllocatedExecutor executor) {
      super(OpType.ALLOCATE, op);
      this.executor = executor;
    }

    /**
     * @return an {@link AllocatedExecutor} allocated by this operation
     */
    public AllocatedExecutor getExecutor() {
      return executor;
    }
  }

  public static final class DeallocateOpResult extends OpResult {
    public DeallocateOpResult(final DeallocateOp op) {
      super(OpType.DEALLOCATE, op);
    }
  }

  public static final class AssociateOpResult extends OpResult {
    private final String executorId;
    private final String tableId;

    public AssociateOpResult(final AssociateOp op, final String executorId, final String tableId) {
      super(OpType.ASSOCIATE, op);
      this.executorId = executorId;
      this.tableId = tableId;
    }

    public String getExecutorId() {
      return executorId;
    }

    public String getTableId() {
      return tableId;
    }
  }

  public static final class UnassociateOpResult extends OpResult {

    private final String executorId;
    private final String tableId;

    public UnassociateOpResult(final UnassociateOp op, final String executorId, final String tableId) {
      super(OpType.UNASSOCIATE, op);
      this.executorId = executorId;
      this.tableId = tableId;
    }

    public String getExecutorId() {
      return executorId;
    }

    public String getTableId() {
      return tableId;
    }
  }

  public static final class SubscribeOpResult extends OpResult {
    public SubscribeOpResult(final SubscribeOp op) {
      super(OpType.SUBSCRIBE, op);
    }
  }

  public static final class UnsubscribeOpResult extends OpResult {
    public UnsubscribeOpResult(final UnsubscribeOp op) {
      super(OpType.UNSUBSCRIBE, op);
    }
  }

  public static final class MoveOpResult extends OpResult {
    private final MigrationResult migrationResult;

    public MoveOpResult(final MoveOp op, final MigrationResult migrationResult) {
      super(OpType.MOVE, op);
      this.migrationResult = migrationResult;
    }

    /**
     * @return an {@link MigrationResult}
     */
    public MigrationResult getMigrationResult() {
      return migrationResult;
    }
  }

  public static final class CreateOpResult extends OpResult {
    private final AllocatedTable table;

    public CreateOpResult(final CreateOp op, final AllocatedTable table) {
      super(OpType.CREATE, op);
      this.table = table;
    }

    /**
     * @return an {@link AllocatedTable} created by this operation
     */
    public AllocatedTable getTable() {
      return table;
    }
  }

  public static final class DropOpResult extends OpResult {
    public DropOpResult(final DropOp op) {
      super(OpType.DROP, op);
    }
  }

  public static final class StartOpResult extends OpResult {
    private final SubmittedTask task;

    public StartOpResult(final StartOp op, final SubmittedTask task) {
      super(OpType.START, op);
      this.task = task;
    }

    /**
     * @return a {@link SubmittedTask} started by this operation
     */
    public SubmittedTask getTask() {
      return task;
    }
  }

  public static final class StopOpResult extends OpResult {
    public StopOpResult(final StopOp op) {
      super(OpType.STOP, op);
    }
  }
}
