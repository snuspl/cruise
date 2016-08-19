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

drop table if exists worker;
drop table if exists server;
drop table if exists custom;

create table worker (
    time double not null,
    id int not null,
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

create table server (
    time double not null,
    id int not null,
    windowIndex int not null,
    numModelBlocks int not null,
    metricWindowMs long not null,
    totalPullProcessed int not null,
    totalPushProcessed int not null,
    totalReqProcessed int not null,
    totalPullProcessingTime double not null,
    totalPushProcessingTime double not null,
    totalReqProcessingTime double not null
);
