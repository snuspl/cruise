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
package edu.snu.cay.services.em.evaluator.api;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import org.apache.reef.wake.EventHandler;

/**
 * An interface that handles msgs about remote access operations.
 * It handles 1) remote op msg that other stores requested to this store
 * and 2) the result msg of remote op that was requested by this store to other stores.
 */
public interface RemoteOpHandler extends EventHandler<AvroElasticMemoryMessage> {
}
