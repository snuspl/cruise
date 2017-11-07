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
package edu.snu.cay.services.et.common.api;

import edu.snu.cay.services.et.avro.ETMsg;
import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

/**
 * Interface to handle {@link ETMsg}.
 */
public interface MessageHandler extends EventHandler<Message<ETMsg>> {
}
