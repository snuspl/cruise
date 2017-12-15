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
package edu.snu.spl.cruise.pregel;

import edu.snu.spl.cruise.common.centcomm.avro.CentCommMsg;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A driver-side message handler that routes messages to {@link PregelMaster}.
 */
public final class DriverSideMsgHandler implements EventHandler<CentCommMsg> {
  private static final Logger LOG = Logger.getLogger(DriverSideMsgHandler.class.getName());

  private final InjectionFuture<PregelMaster> pregelMasterFuture;

  @Inject
  private DriverSideMsgHandler(final InjectionFuture<PregelMaster> pregelMasterFuture) {
    this.pregelMasterFuture = pregelMasterFuture;
  }

  @Override
  public void onNext(final CentCommMsg message) {

    final String sourceId = message.getSourceId().toString();

    LOG.log(Level.INFO, "Received CentComm message {0} from {1}",
        new Object[]{message, sourceId});

    final SuperstepResultMsg resultMsg = AvroUtils.fromBytes(message.getData().array(), SuperstepResultMsg.class);

    pregelMasterFuture.get().onWorkerMsg(sourceId, resultMsg);
  }
}
