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

import edu.snu.cay.services.et.avro.ETMsg;
import edu.snu.cay.services.et.avro.MigrationMsg;
import edu.snu.cay.services.et.avro.TableControlMsg;
import edu.snu.cay.services.et.avro.TableInitAckMsg;
import edu.snu.cay.services.et.common.api.MessageHandler;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A message handler implementation.
 */
@DriverSide
public final class MessageHandlerImpl implements MessageHandler {

  private final InjectionFuture<TableInitializer> tableInitializerFuture;

  @Inject
  private MessageHandlerImpl(final InjectionFuture<TableInitializer> tableInitializerFuture) {
    this.tableInitializerFuture = tableInitializerFuture;
  }

  @Override
  public void onNext(final Message<ETMsg> msg) {
    final ETMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case TableControlMsg:
      onTableControlMsg(innerMsg.getTableControlMsg());
      break;

    case MigrationMsg:
      onMigrationMsg(innerMsg.getMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableControlMsg(final TableControlMsg msg) {
    switch (msg.getType()) {
    case TableInitAckMsg:
      onTableInitAckMsg(msg.getTableInitAckMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableInitAckMsg(final TableInitAckMsg msg) {
    tableInitializerFuture.get().onTableInitAck(msg.getTableId(), msg.getExecutorId());
  }

  private void onMigrationMsg(final MigrationMsg msg) {
    switch (msg.getType()) {
    case DataMovedMsg:
      //onDataMovedMsg();
      break;

    case OwnershipMovedMsg:
      //onOwnership
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }
}
