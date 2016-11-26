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
package edu.snu.cay.common.dataloader;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.data.loading.api.DistributedDataSet;
import org.apache.reef.io.data.loading.api.EvaluatorToPartitionStrategy;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartitionSerializer;
import org.apache.reef.io.data.loading.impl.SingleDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.io.data.loading.impl.DistributedDataSetPartition;
import org.apache.reef.io.data.loading.impl.InputFormatLoadingService;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.io.data.loading.impl.MultiDataCenterEvaluatorToPartitionStrategy;
import org.apache.reef.runtime.common.utils.Constants;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.formats.ConfigurationModule;

/**
 * Builder to create a request to the DataLoadingService.
 * It's resembled from {@link org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder}
 * to separate out its resource request and management part.
 */
public final class DataLoadingRequestBuilder
    implements org.apache.reef.util.Builder<Configuration> {

  // constant used in several places.
  private static final int UNINITIALIZED = -1;
  private int numberOfDesiredSplits = UNINITIALIZED;
  private boolean inMemory = false;
  private ConfigurationModule driverConfigurationModule = null;
  private String inputFormatClass;
  /**
   * Single data center loading strategy flag. Allows to specify if the data
   * will be loaded in machines of a single data center or not. By
   * default, is set to true.
   */
  private boolean singleDataCenterStrategy = true;
  /**
   * Distributed dataset that can contain many distributed partitions.
   */
  private DistributedDataSet distributedDataSet;

  /**
   * The input path of the data to be loaded.
   */
  private String inputPath;

  public DataLoadingRequestBuilder setNumberOfDesiredSplits(final int numberOfDesiredSplits) {
    this.numberOfDesiredSplits = numberOfDesiredSplits;
    return this;
  }

  @SuppressWarnings("checkstyle:hiddenfield")
  public DataLoadingRequestBuilder loadIntoMemory(final boolean inMemory) {
    this.inMemory = inMemory;
    return this;
  }

  public DataLoadingRequestBuilder setDriverConfigurationModule(
      final ConfigurationModule driverConfigurationModule) {
    this.driverConfigurationModule = driverConfigurationModule;
    return this;
  }

  public DataLoadingRequestBuilder setInputFormatClass(
      final Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass.getName();
    return this;
  }

  /**
   * Sets the path of the folder where the data is.
   * Internally, a distributed dataset with a unique partition is created,
   * and {@link SingleDataCenterEvaluatorToPartitionStrategy} is binded.
   *
   * @param inputPath
   *          the input path
   * @return this
   */
  public DataLoadingRequestBuilder setInputPath(final String inputPath) {
    this.inputPath = inputPath;
    this.singleDataCenterStrategy = true;
    return this;
  }

  /**
   * Sets the distributed data set.
   * Internally, a {@link MultiDataCenterEvaluatorToPartitionStrategy} is binded.
   *
   * @param distributedDataSet
   *          the distributed data set
   * @return this
   */
  public DataLoadingRequestBuilder setDistributedDataSet(final DistributedDataSet distributedDataSet) {
    this.distributedDataSet = distributedDataSet;
    this.singleDataCenterStrategy = false;
    return this;
  }

  @Override
  public Configuration build() throws BindException {
    if (this.driverConfigurationModule == null) {
      throw new BindException("Driver Configuration Module is a required parameter.");
    }

    // need to create the distributed data set
    if (this.singleDataCenterStrategy) {
      if (this.inputPath == null) {
        throw new BindException("Should specify an input path.");
      }
      if (this.distributedDataSet != null && !this.distributedDataSet.isEmpty()) {
        throw new BindException("You should either call setInputPath or setDistributedDataSet, but not both");
      }
      // Create a distributed data set with one partition, the splits defined by
      // the user if greater than 0 or no splits, and data to be loaded from
      // anywhere.
      final DistributedDataSet dds = new DistributedDataSet();
      dds.addPartition(DistributedDataSetPartition
          .newBuilder()
          .setPath(inputPath)
          .setLocation(Constants.ANY_RACK)
          .setDesiredSplits(
              numberOfDesiredSplits > 0 ? numberOfDesiredSplits : 0).build());
      this.distributedDataSet = dds;
    } else {
      if (this.inputPath != null) {
        throw new BindException("You should either call setInputPath or setDistributedDataSet, but not both");
      }
    }

    if (this.distributedDataSet == null || this.distributedDataSet.isEmpty()) {
      throw new BindException("Distributed Data Set is a required parameter.");
    }

    if (this.inputFormatClass == null) {
      this.inputFormatClass = TextInputFormat.class.getName();
    }

    final Configuration driverConfiguration = this.driverConfigurationModule.build();

    final JavaConfigurationBuilder jcb =
        Tang.Factory.getTang().newConfigurationBuilder(driverConfiguration);

    jcb.bindNamedParameter(org.apache.reef.io.data.loading.api.DataLoadingRequestBuilder.LoadDataIntoMemory.class,
        Boolean.toString(this.inMemory))
        .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClass);

    for (final DistributedDataSetPartition aDistributedDataSet : this.distributedDataSet) {
      jcb.bindSetEntry(
          DistributedDataSetPartitionSerializer.DistributedDataSetPartitions.class,
          DistributedDataSetPartitionSerializer.serialize(aDistributedDataSet));
    }

    // we do this check for backwards compatibility, if the user defined it
    // wants to use the single data center loading strategy, we bind that implementation.
    if (this.singleDataCenterStrategy) {
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, SingleDataCenterEvaluatorToPartitionStrategy.class);
    } else {
      // otherwise, we bind the strategy that will allow the user to specify
      // which evaluators can load the different partitions in a multi data center network topology
      jcb.bindImplementation(EvaluatorToPartitionStrategy.class, MultiDataCenterEvaluatorToPartitionStrategy.class);
    }

    return jcb.bindImplementation(DataLoadingService.class, InputFormatLoadingService.class).build();
  }
}
