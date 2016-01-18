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
/**
 * A simple example that uses the Parameter Server and AddUpdater.
 * It pushes updates from multiple workers by running UpdaterTask,
 * then pulls and validates the resulting values by running ValidatorTask.
 *
 * The example has two main purposes:
 *   1. It shows how a PS implementation can be configured.
 *   2. It can also work as a simple load generator to test PS implementations.
 */
package edu.snu.cay.services.ps.examples.add;
