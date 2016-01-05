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
package edu.snu.cay.dolphin.core;

import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.util.Optional;

import javax.inject.Inject;

/**
 * Default implementation of {@link CtrlTaskContextIdFetcher}.
 * Uses a {@link InjectionFuture} to fetch the id, in case any injection loops are present.
 */
public final class CtrlTaskContextIdFetcherImpl implements CtrlTaskContextIdFetcher {

  private final InjectionFuture<DolphinDriver> dolphinDriver;

  @Inject
  private CtrlTaskContextIdFetcherImpl(final InjectionFuture<DolphinDriver> dolphinDriver) {
    this.dolphinDriver = dolphinDriver;
  }

  public Optional<String> getCtrlTaskContextId() {
    return Optional.ofNullable(dolphinDriver.get().getCtrlTaskContextId());
  }
}
