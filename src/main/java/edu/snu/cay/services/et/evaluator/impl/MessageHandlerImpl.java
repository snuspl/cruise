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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.services.et.evaluator.api.MessageSender;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A message handler implementation.
 */
@EvaluatorSide
public final class MessageHandlerImpl implements MessageHandler {
  private static final int NUM_OWNERSHIP_UPDATE_THREADS = 4;
  private static final int NUM_TABLE_DROP_THREADS = 4;

  private final ExecutorService ownershipUpdateExecutor = Executors.newFixedThreadPool(NUM_OWNERSHIP_UPDATE_THREADS);
  private final ExecutorService tableDropExecutor = Executors.newFixedThreadPool(NUM_TABLE_DROP_THREADS);

  private final InjectionFuture<Tables> tablesFuture;

  private final ConfigurationSerializer confSerializer;
  private final InjectionFuture<MessageSender> msgSenderFuture;
  private final InjectionFuture<RemoteAccessOpHandler> remoteAccessHandlerFuture;
  private final InjectionFuture<RemoteAccessOpSender> remoteAccessSenderFuture;
  private final InjectionFuture<MigrationExecutor> migrationExecutorFuture;

  @Inject
  private MessageHandlerImpl(final InjectionFuture<Tables> tablesFuture,
                             final ConfigurationSerializer confSerializer,
                             final InjectionFuture<MessageSender> msgSenderFuture,
                             final InjectionFuture<RemoteAccessOpHandler> remoteAccessHandlerFuture,
                             final InjectionFuture<RemoteAccessOpSender> remoteAccessSenderFuture,
                             final InjectionFuture<MigrationExecutor> migrationExecutorFuture) {
    this.tablesFuture = tablesFuture;
    this.confSerializer = confSerializer;
    this.msgSenderFuture = msgSenderFuture;
    this.remoteAccessHandlerFuture = remoteAccessHandlerFuture;
    this.remoteAccessSenderFuture = remoteAccessSenderFuture;
    this.migrationExecutorFuture = migrationExecutorFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {

    final ETMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case TableAccessMsg:
      onTableAccessMsg(innerMsg.getTableAccessMsg());
      break;

    case TableControlMsg:
      onTableControlMsg(innerMsg.getTableControlMsg());
      break;

    case MigrationMsg:
      migrationExecutorFuture.get().onNext(innerMsg.getMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableAccessMsg(final TableAccessMsg msg) {
    final Long opId = msg.getOperationId();
    switch (msg.getType()) {
    case TableAccessReqMsg:
      remoteAccessHandlerFuture.get().onTableAccessReqMsg(opId, msg.getTableAccessReqMsg());
      break;

    case TableAccessResMsg:
      remoteAccessSenderFuture.get().onTableAccessResMsg(opId, msg.getTableAccessResMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableControlMsg(final TableControlMsg msg) {
    final Long opId = msg.getOperationId();
    switch (msg.getType()) {
    case TableInitMsg:
      onTableInitMsg(opId, msg.getTableInitMsg());
      break;

    case TableDropMsg:
      onTableDropMsg(opId, msg.getTableDropMsg());
      break;

    case OwnershipUpdateMsg:
      onOwnershipUpdateMsg(msg.getOwnershipUpdateMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableInitMsg(final long opId, final TableInitMsg msg) {
    try {
      final Configuration tableConf = confSerializer.fromString(msg.getTableConf());
      final List<String> blockOwners = msg.getBlockOwners();
      final String serializedHdfsSplitInfo = msg.getFileSplit();

      final String tableId = serializedHdfsSplitInfo == null ?
          tablesFuture.get().initTable(tableConf, blockOwners) :
          tablesFuture.get().initTable(tableConf, blockOwners, serializedHdfsSplitInfo);

      msgSenderFuture.get().sendTableInitAckMsg(opId, tableId);

    } catch (final IOException e) {
      throw new RuntimeException("IOException while initializing a table", e);
    } catch (final InjectionException e) {
      throw new RuntimeException("Table configuration is incomplete to initialize a table", e);
    } catch (final NetworkException e) {
      throw new RuntimeException(e);
    }
  }

  private void onTableDropMsg(final long opId, final TableDropMsg msg) {
    // remove a table after flushing out all operations for the table in sender and handler
    tableDropExecutor.submit(() -> {
      final String tableId = msg.getTableId();
      remoteAccessSenderFuture.get().waitOpsTobeFlushed(tableId);
      remoteAccessHandlerFuture.get().waitOpsTobeFlushed(tableId);
      tablesFuture.get().remove(tableId);

      try {
        msgSenderFuture.get().sendTableDropAckMsg(opId, tableId);
      } catch (NetworkException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void onOwnershipUpdateMsg(final OwnershipUpdateMsg msg) {
    ownershipUpdateExecutor.submit(() -> {
      try {
        final OwnershipCache ownershipCache = tablesFuture.get().getTableComponents(msg.getTableId())
            .getOwnershipCache();
        ownershipCache.update(msg.getBlockId(), msg.getOldOwnerId(), msg.getNewOwnerId());
      } catch (final TableNotExistException e) {
        throw new RuntimeException(e);
      }
    });
  }
}
