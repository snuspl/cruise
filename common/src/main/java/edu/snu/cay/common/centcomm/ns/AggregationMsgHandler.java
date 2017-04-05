/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.common.centcomm.ns;

import edu.snu.cay.common.aggregation.avro.CentCommMsg;
import edu.snu.cay.common.centcomm.params.AggregationClientHandlers;
import edu.snu.cay.common.centcomm.params.AggregationClientInfo;
import edu.snu.cay.utils.SingleMessageExtractor;
import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Handler for CentCommMsg, which can be used for both aggregation master and slave.
 * Wraps clients' CentCommMsg handlers and routes message to right client handler.
 * Parse strings in {@link AggregationClientInfo} to route CentCommMsg to right handler.
 */
// TODO #352: After REEF-402 is resolved, we can simply use two parallel lists for routing.
public final class AggregationMsgHandler implements EventHandler<Message<CentCommMsg>> {
  private static final Logger LOG = Logger.getLogger(AggregationMsgHandler.class.getName());

  private final Map<String, EventHandler<CentCommMsg>> innerHandlerMap;

  /**
   * Constructor for aggregation message handler.
   * If this class is on the aggregation master, {@code innerHandlers} are master-side message handlers.
   * Otherwise, {@code innerHandlers} are slave-side message handlers.
   * @param clientInfo a set of strings which contains class names of each aggregation client and handlers
   * @param innerHandlers client message handlers, can be both master-side and slave-side handlers
   */
  @Inject
  private AggregationMsgHandler(@Parameter(AggregationClientInfo.class) final Set<String> clientInfo,
                                @Parameter(AggregationClientHandlers.class)
                                final Set<EventHandler<CentCommMsg>> innerHandlers) {
    innerHandlerMap = new HashMap<>();
    for (final String s : clientInfo) {
      final String[] split = s.split("//");
      EventHandler<CentCommMsg> matchingHandler = null;
      for (final EventHandler<CentCommMsg> handler : innerHandlers) {
        if (handler.getClass().getName().equals(split[1])) {
          matchingHandler = handler;
          break;
        } else if (handler.getClass().getName().equals(split[2])) {
          matchingHandler = handler;
          break;
        }
      }
      innerHandlerMap.put(split[0], matchingHandler);
    }
  }

  @Override
  public void onNext(final Message<CentCommMsg> message) {
    LOG.entering(AggregationMsgHandler.class.getSimpleName(), "onNext");

    final CentCommMsg aggregationMessage = SingleMessageExtractor.extract(message);
    final EventHandler<CentCommMsg> handler
        = innerHandlerMap.get(aggregationMessage.getClientClassName().toString());
    if (handler == null) {
      throw new RuntimeException("Unknown aggregation service client " + aggregationMessage.getClientClassName());
    }
    handler.onNext(aggregationMessage);

    LOG.exiting(AggregationMsgHandler.class.getSimpleName(), "onNext");
  }
}
