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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.MessageSenderImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Interface for master to send messages to executors.
 */
@DriverSide
@DefaultImplementation(MessageSenderImpl.class)
public interface MessageSender {

  /**
   * Sends a TableInitMsg that initializes {@link edu.snu.cay.services.et.evaluator.api.Table} in an executor.
   * It includes metadata for initializing a table.
   * If {@code fileSplit} is present, the executor will load file into initialized local blocks.
   */
  void sendTableInitMsg(long opId, String executorId,
                        TableConfiguration tableConf,
                        List<String> blockOwnerList,
                        @Nullable HdfsSplitInfo fileSplit);

  /**
   * Sends a TableDropMsg that deletes the table from the executor.
   */
  void sendTableDropMsg(long opId, String executorId,
                        String tableId);

  /**
   * Sends a OwnershipUpdateMsg that notifies ownership update in other executors.
   */
  void sendOwnershipUpdateMsg(String executorId,
                              String tableId, int blockId,
                              String oldOwnerId, String newOwnerId);

  /**
   * Sends a OwnershipSyncMsg that confirms the deletion of {@code deletedExecutorId}
   * from the ownership cache in an executor of {@code executorId}.
   */
  void sendOwnershipSyncMsg(long opId, String executorId,
                            String tableId, String deletedExecutorId);

  /**
   * Sends a MoveInitMsg that initializes migration process between two executors.
   */
  void sendMoveInitMsg(long opId, String tableId, List<Integer> blockIds,
                       String senderId, String receiverId);
}
