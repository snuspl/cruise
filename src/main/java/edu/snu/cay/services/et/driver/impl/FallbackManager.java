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
package edu.snu.cay.services.et.driver.impl;

import edu.snu.cay.services.et.avro.TableAccessMsg;
import edu.snu.cay.services.et.avro.TableAccessReqMsg;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.driver.api.MessageSender;
import edu.snu.cay.services.et.evaluator.api.BlockPartitioner;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages fallback logic for data access failure, especially in case of the table gets unassociated.
 * If the table exists, the failed data access request is redirected to the owner of the request's key.
 */
final class FallbackManager {
  private static final Logger LOG = Logger.getLogger(FallbackManager.class.getName());
  private static final long RESEND_INTERVAL_MS = 100;

  private final InjectionFuture<MessageSender> messageSenderFuture;
  private final InjectionFuture<TableManager> tableManagerFuture;

  @Inject
  private FallbackManager(final InjectionFuture<MessageSender> messageSenderFuture,
                          final InjectionFuture<TableManager> tableManagerFuture) {
    this.messageSenderFuture = messageSenderFuture;
    this.tableManagerFuture = tableManagerFuture;
  }

  @SuppressWarnings("unchecked")
  void onTableAccessReqMessage(final String srcId, final TableAccessMsg tableAccessMsg) {
    final long opId = tableAccessMsg.getOperationId();
    final TableAccessReqMsg innerMsg = tableAccessMsg.getTableAccessReqMsg();
    final String tableId = innerMsg.getTableId();

    LOG.log(Level.INFO, "The table access request (Table: {0}, opId: {1}, origId: {2}) has failed at {3}." +
            " Will try redirection.", new Object[] {tableId, opId, innerMsg.getOrigId(), srcId});

    try {
      final AllocatedTable table = tableManagerFuture.get().getAllocatedTable(tableId);
      final Injector injector = Tang.Factory.getTang().newInjector(table.getTableConfiguration().getConfiguration());
      final Codec keyCodec = injector.getNamedInstance(KeyCodec.class);

      final BlockPartitioner blockPartitioner = injector.getInstance(BlockPartitioner.class);
      final int blockId = blockPartitioner.getBlockId(keyCodec.decode(innerMsg.getDataKey().getKey().array()));

      while (true) {
        final String executorIdToSendMsg = table.getOwnerId(blockId);

        LOG.log(Level.INFO, "The table access request (Table: {0}, opId: {1}, origId: {2}) will be redirected to {3}.",
            new Object[] {tableId, opId, innerMsg.getOrigId(), executorIdToSendMsg});

        try {
          messageSenderFuture.get().sendTableAccessReqMsg(executorIdToSendMsg, opId, innerMsg);
          break;
        } catch (NetworkException e) {
          LOG.log(Level.WARNING, "NetworkException while sending a msg. Resend", e);
        }

        LOG.log(Level.INFO, "Wait {0} ms before resending a msg", RESEND_INTERVAL_MS);
        try {
          // may not sleep for RESEND_INTERVAL_MS due to interrupt
          Thread.sleep(RESEND_INTERVAL_MS);
        } catch (final InterruptedException e) {
          LOG.log(Level.FINEST, "Interrupted while waiting for ownership cache to be updated", e);
        }
      }

    } catch (TableNotExistException | InjectionException e) {
      LOG.log(Level.INFO, "Redirection of the table access request has failed.", e);
      throw new RuntimeException(e);
    }
  }
}
