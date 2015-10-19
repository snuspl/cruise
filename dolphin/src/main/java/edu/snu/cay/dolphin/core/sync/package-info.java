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
/**
 * Implements a protocol to synchronize between Driver and Controller Task execution.
 * The protocol guarantees that a Driver-side function can execute while the Controller Task waits (or is "paused")
 * before an iteration. The Controller Task is then notified that a synchronized Driver-side execution has taken place.
 */
package edu.snu.cay.dolphin.core.sync;
