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
package edu.snu.spl.cruise.ps.jobserver.driver;

import edu.snu.spl.cruise.ps.core.master.CruiseMaster;
import edu.snu.spl.cruise.ps.CruiseMsg;
import edu.snu.spl.cruise.services.et.evaluator.api.TaskletCustomMsgHandler;
import edu.snu.spl.cruise.utils.AvroUtils;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;

/**
 * A driver-side message handler for JobServer, which manages multiple {@link CruiseMaster}s.
 * Therefore, it routes messages to an appropriate {@link CruiseMaster}
 * based on {@link edu.snu.spl.cruise.ps.CruiseParameters.CruiseJobId} embedded in
 * incoming {@link CruiseMsg}.
 */
public final class DriverSideMsgHandler implements TaskletCustomMsgHandler {

  private final InjectionFuture<JobServerDriver> jobServerDriverFuture;

  @Inject
  private DriverSideMsgHandler(final InjectionFuture<JobServerDriver> jobServerDriverFuture) {
    this.jobServerDriverFuture = jobServerDriverFuture;
  }

  @Override
  public void onNext(final byte[] bytes) {
    final CruiseMsg cruiseMsg = AvroUtils.fromBytes(bytes, CruiseMsg.class);

    final String jobId = cruiseMsg.getJobId().toString();
    final CruiseMaster cruiseMaster = jobServerDriverFuture.get().getCruiseMaster(jobId);
    cruiseMaster.getMsgHandler().onCruiseMsg(cruiseMsg);
  }
}
