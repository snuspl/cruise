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
package edu.snu.cay.services.em.msg.impl;

import edu.snu.cay.services.em.avro.EMMigrationMsg;
import edu.snu.cay.services.em.avro.MigrationMsgType;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class ElasticMemoryCallbackRouterImplTest {

  private ElasticMemoryCallbackRouterImpl callbackRouter;

  @Before
  public void setUp() throws InjectionException {
    callbackRouter = Tang.Factory.getTang().newInjector().getInstance(ElasticMemoryCallbackRouterImpl.class);
  }

  /**
   * Test normal callback register and onCompleted.
   */
  @Test
  public void testCallback() {
    final String operationId = "TEST-callback-000";
    final AtomicInteger numCallbackCalled = new AtomicInteger(0);

    callbackRouter.register(operationId, new EventHandler<EMMigrationMsg>() {
      @Override
      public void onNext(final EMMigrationMsg value) {
        numCallbackCalled.incrementAndGet();
      }
    });
    assertEquals("Callback not yet called", 0, numCallbackCalled.get());

    final EMMigrationMsg msg = EMMigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .setOperationId(operationId)
        .build();

    callbackRouter.onCompleted(msg);
    assertEquals("Callback was called once", 1, numCallbackCalled.get());

    // The callback should fail, with a warning
    callbackRouter.onCompleted(msg);
    assertEquals("Callback was called once", 1, numCallbackCalled.get());
  }

  /**
   * Test callback onCompleted on msg without an Operation ID does not throw an exception.
   */
  @Test
  public void testOnCompletedMsgWithoutOperationId() {
    final EMMigrationMsg msgWithoutOperationId = EMMigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .build();
    callbackRouter.onCompleted(msgWithoutOperationId);
  }

  /**
   * Test callback onCompleted on unregistered msg does not throw an exception.
   */
  @Test
  public void testOnCompletedWithoutUnregisteredMsg() {
    final EMMigrationMsg msgWithoutOperationId = EMMigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .setOperationId("TEST-unregistered-000")
        .build();
    callbackRouter.onCompleted(msgWithoutOperationId);
  }

  /**
   * Test register with same operation ID does not throw an exception,
   * and onCompleted only calls the first callback registered.
   */
  @Test
  public void testRegisterSameOperationId() {
    final String operationId = "TEST-register-000";
    final AtomicBoolean firstCallbackCalled = new AtomicBoolean(false);
    final AtomicBoolean secondCallbackCalled = new AtomicBoolean(false);

    callbackRouter.register(operationId, new EventHandler<EMMigrationMsg>() {
      @Override
      public void onNext(final EMMigrationMsg value) {
        firstCallbackCalled.set(true);
      }
    });

    callbackRouter.register(operationId, new EventHandler<EMMigrationMsg>() {
      @Override
      public void onNext(final EMMigrationMsg value) {
        secondCallbackCalled.set(true);
      }
    });

    final EMMigrationMsg msg = EMMigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .setOperationId(operationId)
        .build();
    callbackRouter.onCompleted(msg);
    callbackRouter.onCompleted(msg); // Should not call any callback.
    assertTrue("First callback called", firstCallbackCalled.get());
    assertFalse("Second callback not called", secondCallbackCalled.get());
  }

  /**
   * Test callback onCompleted on an operation registered with a null callback does not throw an exception.
   */
  @Test
  public void testRegisterNullCallback() {
    final String operationId = "TEST-null-callback-000";
    callbackRouter.register(operationId, null);

    final EMMigrationMsg msg = EMMigrationMsg.newBuilder()
        .setType(MigrationMsgType.ResultMsg)
        .setOperationId(operationId)
        .build();
    callbackRouter.onCompleted(msg);
  }
}
