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

/*
 * This initializes the database.
 * The database contains three tables: worker, server, metrics.
 * Table Worker and Server are initialized here, since we know what metrics are coming,
 * Table metrics will be initialized dynamically at runtime.
 */

drop table if exists workers;
drop table if exists servers;
drop table if exists metrics;

create table workers (
    id int not null,
/*    metrics varchar(255) not null, */
    itrIdx int not null,
    numDataBlocks int not null,
    processedDataItemCount int not null,
    numMiniBatchPerItr int not null,
    totalTime double not null,
    totalCompTime double not null,
    totalPushTime double not null,
    totalPullTime double not null,
    avgPushTime double not null,
    avgPullTime double not null
);

create table servers (
    id int not null,
/*    metrics varchar(255) not null, */
    windowIndex int not null,
    numPartitionBlocks int not null,
    metricWindowMs long not null,
    avgPullProcessingTime double not null,
    avgPushProcessingTime double not null,
    avgReqProcessingTime double not null
);

