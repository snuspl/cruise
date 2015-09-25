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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.services.em.avro.*;
import edu.snu.cay.services.em.msg.api.ElasticMemoryCallbackRouter;
import edu.snu.cay.services.em.trace.HTraceUtils;
import edu.snu.cay.services.em.utils.SingleMessageExtractor;
import org.htrace.Trace;
import org.htrace.TraceScope;
import org.htrace.TraceInfo;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Driver-side message handler.
 * Currently does nothing, but we need this class as a placeholder to
 * instantiate NetworkService.
 */
@DriverSide
final class ElasticMemoryMsgHandler implements EventHandler<Message<AvroElasticMemoryMessage>> {
  private static final Logger LOG = Logger.getLogger(ElasticMemoryMsgHandler.class.getName());

  private static final String ON_REGIS_MSG = "onRegisMsg";
  private static final String ON_RESULT_MSG = "onResultMsg";
  private static final String ON_UPDATE_ACK_MSG = "onUpdateAckMsg";

  private final ElasticMemoryCallbackRouter callbackRouter;
  private final PartitionManager partitionManager;
  private final MigrationManager migrationManager;

  @Inject
  private ElasticMemoryMsgHandler(final ElasticMemoryCallbackRouter callbackRouter,
                                  final PartitionManager partitionManager,
                                  final MigrationManager migrationManager) {
    this.callbackRouter = callbackRouter;
    this.partitionManager = partitionManager;
    this.migrationManager = migrationManager;
  }

  @Override
  public void onNext(final Message<AvroElasticMemoryMessage> msg) {
    LOG.entering(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);

    final AvroElasticMemoryMessage innerMsg = SingleMessageExtractor.extract(msg);
    switch (innerMsg.getType()) {
    case RegisMsg:
      onRegisMsg(innerMsg);
      break;

    case ResultMsg:
      onResultMsg(innerMsg);
      break;

    case UpdateAckMsg:
      onUpdateAckMsg(innerMsg);
      break;

    default:
      throw new RuntimeException("Unexpected message: " + msg);
    }

    LOG.exiting(ElasticMemoryMsgHandler.class.getSimpleName(), "onNext", msg);
  }


  private void onRegisMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onRegisMsgScope = Trace.startSpan(ON_REGIS_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {

      final RegisMsg regisMsg = msg.getRegisMsg();

      // register a partition for the evaluator as specified in the message
      partitionManager.registerPartition(msg.getSrcId().toString(),
          regisMsg.getDataType().toString(), regisMsg.getIdRange().getMin(), regisMsg.getIdRange().getMax());
    }
  }

  private void onResultMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onResultMsgScope = Trace.startSpan(ON_RESULT_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final Result result = msg.getResultMsg().getResult();
      switch (result) {
      case SUCCESS:
        // After data is sent, EM should wait for user's approval before applying those changes.
        // Once EM can make sure there is no race condition even without this barrier, this step should be removed.
        final String operationId = msg.getOperationId().toString();
        migrationManager.waitUpdate(operationId);
        break;
      case FAILURE:
        // TODO #90: We need to handle the failure and notify the failure via callback.
        break;
      default:
        throw new RuntimeException("Undefined result: " + result);
      }
    }
  }

   private void onUpdateAckMsg(final AvroElasticMemoryMessage msg) {
    try (final TraceScope onUpdateAckMsgScope =
             Trace.startSpan(ON_UPDATE_ACK_MSG, HTraceUtils.fromAvro(msg.getTraceInfo()))) {
      final UpdateResult updateResult = msg.getUpdateAckMsg().getResult();
      final String operationId = msg.getOperationId().toString();

      switch (updateResult) {

      // After receiver updates its state, EM guarantees that the partition can be accessed in the receiver side.
      // So update the partition state and update the sender.
      case RECEIVER_UPDATED:
        final TraceInfo traceInfo = TraceInfo.fromSpan(onUpdateAckMsgScope.getSpan());
        final boolean partitionMoved = migrationManager.movePartition(operationId, traceInfo);

        if (partitionMoved) {
          migrationManager.updateSender(operationId, traceInfo);
        } else {
          // TODO #90: Moving partition failed. We need to handle the failure and notify the failure via callback.
        }
        break;

      case SENDER_UPDATED:
        migrationManager.finishMigration(operationId);
        callbackRouter.onCompleted(msg);
        break;

      case FAILED:
        // TODO #90: We need to handle the failure and notify the failure via callback.
        break;

      default:
        throw new RuntimeException("Undefined result: " + updateResult);
      }
    }
  }
}
