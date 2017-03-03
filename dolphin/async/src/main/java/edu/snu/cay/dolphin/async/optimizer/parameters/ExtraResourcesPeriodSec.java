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
package edu.snu.cay.dolphin.async.optimizer.parameters;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

@NamedParameter(doc = "The period (and duration) of extra resources. Note that it only works when the number of " +
    "extra resources is set positive.",
    short_name = "extra_resources_period_sec", default_value = "0")
public class ExtraResourcesPeriodSec implements Name<Long> {
}
