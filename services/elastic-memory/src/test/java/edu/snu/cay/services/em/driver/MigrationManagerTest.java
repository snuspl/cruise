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

import edu.snu.cay.services.em.avro.UnitIdPair;
import edu.snu.cay.services.em.avro.UpdateResult;
import edu.snu.cay.services.em.msg.api.ElasticMemoryMsgSender;
import org.apache.commons.lang.math.LongRange;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.htrace.TraceInfo;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;


/**
 * Tests MigrationManager manages the states correctly.
 */
@Unit
public final class MigrationManagerTest {
  private MigrationManager migrationManager;
  private TraceInfo traceInfo = new TraceInfo(0, 0);

  @Before
  public void setUp() {
    try {
      // MockMsgSender for Tang because it tries to find the actual parameters such as NameResolveNameServerAddr.
      final Configuration conf = Tang.Factory.getTang().newConfigurationBuilder()
          .bindImplementation(ElasticMemoryMsgSender.class, MockMsgSender.class)
          .build();

      migrationManager = Tang.Factory.getTang().newInjector(conf).getInstance(MigrationManager.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Injection failed while setting up the test", e);
    }
  }

  /**
   * Tests that end-to-end scenario that moves a range.
   * Cannot mock the object because they are injected via Tang.
   */
  @Test
  public void testEndToEnd() {
    final String opId = "OP";
    final String sender = "SENDER";
    final String receiver = "RECEIVER";
    final String dataType = "DATA";

    final Set<LongRange> ranges = new HashSet<>();
    ranges.add(new LongRange(0, 1));

    // 1. Start the migration (called by ElasticMemory.move())
    migrationManager.startMigration(opId, sender, receiver, dataType, ranges, traceInfo);

    // 2. On data sent (called by ElasticMemoryMsgHandler.on)
    migrationManager.waitUpdate(opId, ranges);

    final ExecutorService e = Executors.newSingleThreadExecutor();
    e.submit(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(10);
          // Move partition will fail
//          doThrow(new RuntimeException()).when(migrationManager).movePartition(opId, traceInfo);
          // TODO #96: Throw RuntimeException?
          migrationManager.movePartition(opId, traceInfo);

          // Update the senders when move Succeed
          migrationManager.updateSender(opId, traceInfo);

          // Finish the migration
          migrationManager.finishMigration(opId);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    });

    migrationManager.applyUpdates(traceInfo);
    System.out.println("Updates are applied");
  }
}

/**
 * For test only. This sender implementation does not send any message.
 */
final class MockMsgSender implements ElasticMemoryMsgSender {
  @Inject
  private MockMsgSender() {
  }

  @Override
  public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId,
                          final Set<LongRange> idRangeSet, final String operationId,
                          @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendCtrlMsg(final String destId, final String dataType, final String targetEvalId, final int numUnits,
                          final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendDataMsg(final String destId, final String dataType, final List<UnitIdPair> unitIdPairList,
                          final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendResultMsg(final boolean success, final String dataType, final Set<LongRange> idRangeSet,
                            final String operationId, @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendRegisMsg(final String dataType, final long unitStartId, final long unitEndId,
                           @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendUpdateMsg(final String destId, final String operationId,
                            @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }

  @Override
  public void sendUpdateAckMsg(final String operationId, final UpdateResult result,
                               @Nullable final TraceInfo parentTraceInfo) {
    // Do nothing.
  }
}
