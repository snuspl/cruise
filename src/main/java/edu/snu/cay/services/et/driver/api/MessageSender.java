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
import edu.snu.cay.services.et.avro.TableAccessReqMsg;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import edu.snu.cay.services.et.driver.impl.MessageSenderImpl;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.tang.annotations.DefaultImplementation;

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
   */
  void sendTableInitMsg(long opId, String executorId,
                        TableConfiguration tableConf,
                        List<String> blockOwnerList);

  /**
   * Sends a TableLoadMsg that lets an executor loads a given file split into a table.
   */
  void sendTableLoadMsg(long opId, String executorId,
                        String tableId,
                        HdfsSplitInfo hdfsSplitInfo);

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

  /**
   * Sends a message for starting metric collection with a given configuration at an executor.
   */
  void sendMetricStartMsg(String executorId, String serializedMetricConf);

  /**
   * Sends a message for stopping metric collection at an executor.
   */
  void sendMetricStopMsg(String executorId);

  /**
   * Sends a TableAccessReqMsg, which redirects the failed access request to the up-to-date owner if possible.
   */
  void sendTableAccessReqMsg(String destId, long opId, TableAccessReqMsg tableAccessReqMsg) throws NetworkException;
}
