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

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitInfoSerializer;
import edu.snu.cay.services.et.avro.*;
import edu.snu.cay.services.et.common.api.NetworkConnection;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.api.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A message sender implementation.
 */
@DriverSide
public final class MessageSenderImpl implements MessageSender {
  private final NetworkConnection<ETMsg> networkConnection;
  private final ConfigurationSerializer confSerializer;

  @Inject
  private MessageSenderImpl(final NetworkConnection<ETMsg> networkConnection,
                            final ConfigurationSerializer confSerializer) {
    this.networkConnection = networkConnection;
    this.confSerializer = confSerializer;
  }

  @Override
  public void sendTableInitMsg(final String executorId,
                               final TableConfiguration tableConf,
                               final List<String> blockOwnerList,
                               final int revision,
                               final Optional<HdfsSplitInfo> fileSplit) {
    final List<CharSequence> blockOwnerListToSend = new ArrayList<>(blockOwnerList);

    final ETMsg msg = ETMsg.newBuilder()
        .setType(ETMsgType.TableControlMsg)
        .setTableControlMsg(
            TableControlMsg.newBuilder()
                .setType(TableControlMsgType.TableInitMsg)
                .setTableInitMsg(
                    TableInitMsg.newBuilder()
                        .setTableConf(confSerializer.toString(tableConf.getConfiguration()))
                        .setBlockOwners(blockOwnerListToSend)
                        .setRevision(revision)
                        .setFileSplit(fileSplit.isPresent() ?
                            HdfsSplitInfoSerializer.serialize(fileSplit.get()) : null)
                        .build()
                ).build()
        ).build();

    try {
      networkConnection.send(executorId, msg);
    } catch (final NetworkException e) {
      throw new RuntimeException("NetworkException while sending TableInit message", e);
    }
  }

  @Override
  public void sendOwnershipUpdateMsg(final String executorId,
                                     final String tableId, final int blockId,
                                     final String newOwnerId,
                                     final int revision) {

  }

  @Override
  public void sendMoveInitMsg(final String tableId, final List<Integer> blockIds,
                              final String senderId, final String receiverId) {

  }
}
