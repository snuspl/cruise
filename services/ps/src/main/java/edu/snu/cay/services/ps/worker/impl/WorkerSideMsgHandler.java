/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.services.ps.worker.impl;

import edu.snu.cay.services.ps.avro.*;
import edu.snu.cay.services.ps.driver.impl.EMRoutingTable;
import edu.snu.cay.services.ps.PSParameters.KeyCodecName;
import edu.snu.cay.services.ps.PSParameters.ValueCodecName;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.worker.api.AsyncWorkerHandler;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Logger;

/**
 * Worker-side Parameter Server message handler.
 */
@EvaluatorSide
public final class WorkerSideMsgHandler<K, P, V> implements EventHandler<Message<AvroPSMsg>> {
  private static final Logger LOG = Logger.getLogger(WorkerSideMsgHandler.class.getName());

  /**
   * This evaluator's asynchronous handler that is expecting Parameter Server pull message results.
   */
  private final AsyncWorkerHandler<K, V> asyncWorkerHandler;

  private final ServerResolver serverResolver;

  /**
   * Codec for decoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for decoding PS values.
   */
  private final Codec<V> valueCodec;

  @Inject
  private WorkerSideMsgHandler(final AsyncWorkerHandler<K, V> asyncWorkerHandler,
                               final ServerResolver serverResolver,
                               @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                               @Parameter(ValueCodecName.class) final Codec<V> valueCodec) {
    this.asyncWorkerHandler = asyncWorkerHandler;
    this.serverResolver = serverResolver;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
  }

  /**
   * Hand over values given from the server to {@link AsyncWorkerHandler}.
   * Throws an exception if messages of an unexpected type arrive.
   */
  @Override 
  public void onNext(final Message<AvroPSMsg> msg) {
    LOG.entering(WorkerSideMsgHandler.class.getSimpleName(), "onNext");

    final AvroPSMsg innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case ReplyMsg:
      onReplyMsg(innerMsg.getReplyMsg());
      break;

    case WorkerRegisterReplyMsg:
      onRoutingTableReplyMsg(innerMsg.getWorkerRegisterReplyMsg());
      break;

    case RoutingTableUpdateMsg:
      onRoutingTableUpdateMsg(innerMsg.getRoutingTableUpdateMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message type: " + innerMsg.getType().toString());
    }

    LOG.exiting(WorkerSideMsgHandler.class.getSimpleName(), "onNext");
  }

  private void onRoutingTableReplyMsg(final WorkerRegisterReplyMsg workerRegisterReplyMsg) {
    final List<IdMapping> idMappings = workerRegisterReplyMsg.getIdMappings();
    final int numTotalBlocks = workerRegisterReplyMsg.getNumTotalBlocks();

    final Map<Integer, Set<Integer>> storeIdToBlockIds  = new HashMap<>(idMappings.size());
    final Map<Integer, String> storeIdToEndpointId = new HashMap<>(idMappings.size());

    for (final IdMapping idMapping : idMappings) {
      final int memoryStoreId = idMapping.getMemoryStoreId();
      final List<Integer> blockIds = idMapping.getBlockIds();
      final String endpointId = idMapping.getEndpointId().toString();
      storeIdToBlockIds.put(memoryStoreId, new HashSet<>(blockIds));
      storeIdToEndpointId.put(memoryStoreId, endpointId);
    }

    serverResolver.initRoutingTable(new EMRoutingTable(storeIdToBlockIds, storeIdToEndpointId, numTotalBlocks));
  }

  private void onRoutingTableUpdateMsg(final RoutingTableUpdateMsg routingTableUpdateMsg) {
    final int oldOwnerId = routingTableUpdateMsg.getOldOwnerId();
    final int newOwnerId = routingTableUpdateMsg.getNewOwnerId();
    final String newEvalId = routingTableUpdateMsg.getNewEvalId().toString();
    final List<Integer> blockIds = routingTableUpdateMsg.getBlockIds();

    serverResolver.updateRoutingTable(new EMRoutingTableUpdateImpl(oldOwnerId, newOwnerId, newEvalId, blockIds));
  }

  private void onReplyMsg(final ReplyMsg replyMsg) {
    final K key = keyCodec.decode(replyMsg.getKey().array());
    final V value = valueCodec.decode(replyMsg.getValue().array());
    asyncWorkerHandler.processReply(key, value);
  }
}
