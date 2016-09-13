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
import edu.snu.cay.services.ps.PSParameters.PreValueCodecName;
import edu.snu.cay.services.ps.PSParameters.ValueCodecName;
import edu.snu.cay.services.ps.common.resolver.ServerResolver;
import edu.snu.cay.services.ps.worker.api.WorkerHandler;
import edu.snu.cay.utils.SingleMessageExtractor;
import edu.snu.cay.utils.trace.HTraceUtils;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.htrace.Trace;
import org.htrace.TraceInfo;
import org.htrace.TraceScope;

import javax.annotation.Nullable;
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
  private final WorkerHandler<K, P, V> workerHandler;

  private final ServerResolver serverResolver;

  /**
   * Codec for decoding PS keys.
   */
  private final Codec<K> keyCodec;

  /**
   * Codec for decoding PS preValues.
   */
  private final Codec<P> preValueCodec;

  /**
   * Codec for decoding PS values.
   */
  private final Codec<V> valueCodec;

  @Inject
  private WorkerSideMsgHandler(final WorkerHandler<K, P, V> workerHandler,
                               final ServerResolver serverResolver,
                               @Parameter(KeyCodecName.class)final Codec<K> keyCodec,
                               @Parameter(PreValueCodecName.class)final Codec<P> preValueCodec,
                               @Parameter(ValueCodecName.class) final Codec<V> valueCodec) {
    this.workerHandler = workerHandler;
    this.serverResolver = serverResolver;
    this.keyCodec = keyCodec;
    this.preValueCodec = preValueCodec;
    this.valueCodec = valueCodec;

    Trace.setProcessId("parameter_worker");
  }

  /**
   * Hand over values given from the server to {@link WorkerHandler}.
   * Throws an exception if messages of an unexpected type arrive.
   */
  @Override 
  public void onNext(final Message<AvroPSMsg> msg) {
    LOG.entering(WorkerSideMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroPSMsg innerMsg = SingleMessageExtractor.extract(msg);
    final TraceInfo traceInfo = HTraceUtils.fromAvro(innerMsg.getTraceInfo());
    switch (innerMsg.getType()) {
    case PullReplyMsg:
      onPullReplyMsg(innerMsg.getPullReplyMsg(), traceInfo);
      break;

    case PushRejectMsg:
      onPushRejectMsg(innerMsg.getPushRejectMsg());
      break;

    case PullRejectMsg:
      onPullRejectMsg(innerMsg.getPullRejectMsg());
      break;

    case WorkerRegisterReplyMsg:
      onRoutingTableReplyMsg(innerMsg.getWorkerRegisterReplyMsg());
      break;

    case RoutingTableUpdateMsg:
      onRoutingTableUpdateMsg(innerMsg.getRoutingTableUpdateMsg(), traceInfo);
      break;

    case RoutingTableSyncMsg:
      onRoutingTableSyncMsg(innerMsg.getRoutingTableSyncMsg());
      break;

    default:
      throw new RuntimeException("Unexpected message type: " + innerMsg.getType().toString());
    }

    LOG.exiting(WorkerSideMsgHandler.class.getSimpleName(), "onNext");
  }

  private void onRoutingTableReplyMsg(final WorkerRegisterReplyMsg workerRegisterReplyMsg) {
    final List<IdMapping> idMappings = workerRegisterReplyMsg.getIdMappings();

    final Map<Integer, Set<Integer>> storeIdToBlockIds  = new HashMap<>(idMappings.size());
    final Map<Integer, String> storeIdToEndpointId = new HashMap<>(idMappings.size());

    for (final IdMapping idMapping : idMappings) {
      final int memoryStoreId = idMapping.getMemoryStoreId();
      final List<Integer> blockIds = idMapping.getBlockIds();
      final String endpointId = idMapping.getEndpointId().toString();
      storeIdToBlockIds.put(memoryStoreId, new HashSet<>(blockIds));
      storeIdToEndpointId.put(memoryStoreId, endpointId);
    }

    serverResolver.initRoutingTable(new EMRoutingTable(storeIdToBlockIds, storeIdToEndpointId));
  }

  private void onRoutingTableUpdateMsg(final RoutingTableUpdateMsg routingTableUpdateMsg,
                                       @Nullable final TraceInfo traceInfo) {
    Trace.setProcessId("worker");
    try (final TraceScope onRoutingTableUpdateMsgScope = Trace.startSpan("on_routing_table_update_msg", traceInfo)) {

      final int oldOwnerId = routingTableUpdateMsg.getOldOwnerId();
      final int newOwnerId = routingTableUpdateMsg.getNewOwnerId();
      final String newEvalId = routingTableUpdateMsg.getNewEvalId().toString();
      final int blockId = routingTableUpdateMsg.getBlockId();

      serverResolver.updateRoutingTable(new EMRoutingTableUpdateImpl(oldOwnerId, newOwnerId, newEvalId, blockId));
    }
  }

  private void onRoutingTableSyncMsg(final RoutingTableSyncMsg routingTableSyncMsg) {
    final String serverId = routingTableSyncMsg.getServerId().toString();

    serverResolver.syncRoutingTable(serverId);
  }

  private void onPullReplyMsg(final PullReplyMsg pullReplyMsg, @Nullable final TraceInfo traceInfo) {
    try (final TraceScope onPullReplyScope = Trace.startSpan("on_pull_reply", traceInfo)) {
      final K key = keyCodec.decode(pullReplyMsg.getKey().array());
      final V value = valueCodec.decode(pullReplyMsg.getValue().array());
      final int requestId = pullReplyMsg.getRequestId();
      final long serverProcessingTime = pullReplyMsg.getServerProcessingTime();
      workerHandler.processPullReply(key, value, requestId, serverProcessingTime,
          TraceInfo.fromSpan(onPullReplyScope.getSpan()));
    }
  }

  private void onPushRejectMsg(final PushRejectMsg pushRejectMsg) {
    final K key = keyCodec.decode(pushRejectMsg.getKey().array());
    final P preValue = preValueCodec.decode(pushRejectMsg.getPreValue().array());
    workerHandler.processPushReject(key, preValue);
  }

  private void onPullRejectMsg(final PullRejectMsg pullRejectMsg) {
    final K key = keyCodec.decode(pullRejectMsg.getKey().array());
    final int requestId = pullRejectMsg.getRequestId();
    workerHandler.processPullReject(key, requestId);
  }
}
