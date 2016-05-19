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
package edu.snu.cay.services.em.driver.impl;

import edu.snu.cay.services.em.avro.AvroElasticMemoryMessage;
import edu.snu.cay.services.em.driver.api.EMDeleteExecutor;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Default implementation of EMDeleteExecutor.
 */
public final class EMDeleteExecutorImpl implements EMDeleteExecutor {

  @Inject
  private EMDeleteExecutorImpl() {

  }

  @Override
  public boolean execute(final String activeContextId, final EventHandler<AvroElasticMemoryMessage> callback) {
    throw new UnsupportedOperationException();
  }
}
