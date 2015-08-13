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
package edu.snu.cay.services.em.msg.api;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.msg.impl.ElasticMemoryCallbackRouterImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.EventHandler;

/**
 * Routes callbacks for Elastic Memory operations by their operationId.
 * Elastic Memory registers each callback when the operation is called, and routes to that callback's onNext method
 * when the operation is completed.
 */
@DefaultImplementation(ElasticMemoryCallbackRouterImpl.class)
public interface ElasticMemoryCallbackRouter {

  /**
   * Register a new callback for an EM operation.
   * @param operationId A unique ID for the EM operation.
   * @param callback The handler to be called when operation is complete.
   */
  void register(String operationId, EventHandler<AvroElasticMemoryMessage> callback);

  /**
   * Call the registered callback for a completed EM operation.
   * @param msg The message that indicates a completed EM operation.
   */
  void onCompleted(AvroElasticMemoryMessage msg);
}
