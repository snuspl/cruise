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

import edu.snu.cay.services.em.evaluator.impl.DataOperation;
import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;

/**
 * Non-user side interface of memory store including APIs for handling operations from remote request.
 */
@EvaluatorSide
@Private
public interface RemoteAccessibleMemoryStore<K> extends MemoryStore<K>, EventHandler<DataOperation> {
}
