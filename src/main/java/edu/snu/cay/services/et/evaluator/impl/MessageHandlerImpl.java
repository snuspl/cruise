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
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.io.IOException;

/**
 * A message handler implementation.
 */
@EvaluatorSide
public final class MessageHandlerImpl implements MessageHandler {

  private final Tables tables;

  private final ConfigurationSerializer confSerializer;
  private final InjectionFuture<MessageSender> msgSenderFuture;

  @Inject
  private MessageHandlerImpl(final Tables tables,
                             final ConfigurationSerializer confSerializer,
                             final InjectionFuture<MessageSender> msgSenderFuture) {
    this.tables = tables;
    this.confSerializer = confSerializer;
    this.msgSenderFuture = msgSenderFuture;
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
      onMigrationMsg(innerMsg.getMigrationMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableAccessMsg(final TableAccessMsg msg) {
    switch (msg.getType()) {
    case TableAccessReqMsg:
      //onTableAccessReqMsg(msg);
      break;

    case TableAccessResMsg:
      //onTableAccessResMsg(msg)
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

  }

  private void onTableControlMsg(final TableControlMsg msg) {
    switch (msg.getType()) {
    case TableInitMsg:
      onTableInitMsg(msg.getTableInitMsg());
      break;

    case OwnershipUpdateMsg:
      //onOwnershipUpdateMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }

  private void onTableInitMsg(final TableInitMsg msg) {
    try {
      final Optional<String> serializedHdfsSplitInfo;
      if (msg.getFileSplit() == null) {
        serializedHdfsSplitInfo = Optional.empty();
      } else {
        serializedHdfsSplitInfo = Optional.of(msg.getFileSplit().toString());
      }
      final String tableId = tables.initTable(confSerializer.fromString(msg.getTableConf().toString()),
          msg.getBlockOwners(), msg.getRevision(), serializedHdfsSplitInfo);

      msgSenderFuture.get().sendTableInitAckMsg(tableId);

    } catch (final IOException e) {
      throw new RuntimeException("IOException while initialize a table", e);
    } catch (final InjectionException e) {
      throw new RuntimeException("Table configuration is incomplete to initialize a table", e);
    }
  }

  private void onMigrationMsg(final MigrationMsg msg) {
    switch (msg.getType()) {
    case DataMsg:
      //onDataMsg(msg);
      break;

    case DataAckMsg:
      //onDataAckMsg(msg);
      break;

    case OwnershipMsg:
      //onOwnershipMsg(msg);
      break;

    case OwnershipAckMsg:
      //onOwnershipAckMsg(msg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }
  }
}
